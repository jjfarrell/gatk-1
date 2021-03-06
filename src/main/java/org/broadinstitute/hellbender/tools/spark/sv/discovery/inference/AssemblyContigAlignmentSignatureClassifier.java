package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.*;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public final class AssemblyContigAlignmentSignatureClassifier {

    static final int ALIGNMENT_MAPQUAL_THREHOLD = 20;
    static final int ALIGNMENT_READSPAN_THRESHOLD = 10;

    /**
     * Splits the {@code input} RDD into two based on a filtering predicate.
     * @param input              input RDD to be split
     * @param predicate          filtering criteria
     * @return a pair of RDD that where the first has all its elements evaluate to "true" given the predicate,
     *         and the second is the compliment
     */
    private static <T> Tuple2<JavaRDD<T>, JavaRDD<T>> split(final JavaRDD<T> input, final Function<T, Boolean> predicate) {

        final JavaRDD<T> first = input.filter(predicate);
        final JavaRDD<T> second = input.filter(t -> !predicate.call(t));

        return new Tuple2<>(first, second);
    }

    public enum RawTypes {
        InsDel,                 // 2 alignments, indicating ins/del, simple duplication expansion/contractions
        IntraChrStrandSwitch,   // breakpoint for events involving intra-chromosome strand switch, particularly for inversion breakpoint suspects
        MappedInsertionBkpt,    // breakpoint for events involving suspected insertions where the inserted sequence could be mapped to either an 1) same-chromosome location without strand switch (tandem duplication suspect) OR 2) diff-chromosome location (MEI/dispersed dup suspect with or without strand switch)
        Cpx,                    // complex sv that seemingly have the complete event assembled
        Incomplete,             // alignment signature indicates the complete picture is not inferable from alignment signature alone
        Ambiguous,              // assembly contig has more than 1 best alignment configuration hence painting multiple pictures
        MisAssemblySuspect;     // suspected to be misassembly due to alignment signature that despite multiple alignments, no or only 1 good alignment
    }

    public static EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> classifyContigs(final JavaRDD<AssemblyContigWithFineTunedAlignments> contigs,
                                                                                                    final Broadcast<SAMSequenceDictionary> broadcastSequenceDictionary,
                                                                                                    final Logger toolLogger) {

        // long reads with only 1 best configuration
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> split =
                split(contigs, AssemblyContigWithFineTunedAlignments::hasEquallyGoodAlnConfigurations);

        final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithOnlyOneBestConfig = split._2;
        // primary split between 2 vs more than 2 alignments
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> twoAlignmentsOrMore =
                split(contigsWithOnlyOneBestConfig, AssemblyContigWithFineTunedAlignments::hasOnly2GoodAlignments);

        // special treatment for alignments with more than 2 alignments
        final JavaRDD<AssemblyContigWithFineTunedAlignments> moreThanTwoAlignments = twoAlignmentsOrMore._2;
        final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> contigsByRawTypesFromMultiAlignment = new EnumMap<>(RawTypes.class);
        final MultipleAlignmentReclassificationResults multipleAlignmentReclassificationResults =
                reClassifyContigsWithMultipleAlignments(moreThanTwoAlignments,
                        ALIGNMENT_MAPQUAL_THREHOLD, ALIGNMENT_READSPAN_THRESHOLD);
        contigsByRawTypesFromMultiAlignment.put(RawTypes.MisAssemblySuspect,
                multipleAlignmentReclassificationResults.nonInformativeContigs);
        contigsByRawTypesFromMultiAlignment.put(RawTypes.Incomplete,
                multipleAlignmentReclassificationResults.contigsWithMoreThanTwoGoodAlignmentsButIncompletePicture);
        contigsByRawTypesFromMultiAlignment.put(RawTypes.Cpx,
                multipleAlignmentReclassificationResults.contigsWithMoreThanTwoGoodAlignments);

        // merge the two sources of contigs with two alignments
        final JavaRDD<AssemblyContigWithFineTunedAlignments> twoAlignmentsUnion =
                twoAlignmentsOrMore._1.union(multipleAlignmentReclassificationResults.contigsWithTwoGoodAlignments);

        final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> contigsByRawTypesFromTwoAlignment =
                processContigsWithTwoAlignments(twoAlignmentsUnion, broadcastSequenceDictionary);

        final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> result =
                mergeMaps(contigsByRawTypesFromMultiAlignment, contigsByRawTypesFromTwoAlignment);

        // TODO: 11/21/17 later we may decide to salvage ambiguous contigs with some heuristic logic, but later
        // long reads with more than 1 best configurations, i.e. ambiguous raw types
        final JavaRDD<AssemblyContigWithFineTunedAlignments> ambiguous = split._1;
        result.put(RawTypes.Ambiguous, ambiguous);

        return result;
    }

    private static EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> mergeMaps(
            final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> one,
            final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> two) {

        final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> result = new EnumMap<>(RawTypes.class);
        for (final RawTypes type: RawTypes.values()) {
            final JavaRDD<AssemblyContigWithFineTunedAlignments> first = one.get(type);
            final JavaRDD<AssemblyContigWithFineTunedAlignments> second = two.get(type);

            if (first == null) {
                if (second != null) {
                    result.put(type, second);
                }
            } else {
                result.put(type, second == null ? first : first.union(second));
            }
        }
        return result;
    }

    //==================================================================================================================
    // FOR ASSEMBLY CONTIGS WITH TWO ALIGNMENTS

    /**
     * TODO:
     * Logic similar to {@link BreakpointsInference#inferFromSimpleChimera(ChimericAlignment)}, and redundant,
     * finished its historical value hence should be removed.
     */
    static EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> processContigsWithTwoAlignments(
            final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithOnlyOneBestConfigAnd2AI,
            final Broadcast<SAMSequenceDictionary> broadcastSequenceDictionary) {

        final EnumMap<RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>> contigsByRawTypes = new EnumMap<>(RawTypes.class);

        // split between the case where both alignments has unique ref span or not
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> hasFullyContainedRefSpanOrNot =
                split(contigsWithOnlyOneBestConfigAnd2AI, AssemblyContigWithFineTunedAlignments::hasIncompletePictureFromTwoAlignments);
        contigsByRawTypes.put(RawTypes.Incomplete, hasFullyContainedRefSpanOrNot._1);

        // split between same chromosome mapping or not
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> sameChrOrNot =
                split(hasFullyContainedRefSpanOrNot._2, AssemblyContigWithFineTunedAlignments::firstAndLastAlignmentMappedToSameChr);
        final JavaRDD<AssemblyContigWithFineTunedAlignments> diffChrBreakpoints = sameChrOrNot._2;

        // split between strand switch or not (NOTE BOTH SAME CHROMOSOME MAPPING)
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> strandSwitchOrNot =
                split(sameChrOrNot._1, AssemblyContigAlignmentSignatureClassifier::indicatesIntraChrStrandSwitchBkpts);
        contigsByRawTypes.put(RawTypes.IntraChrStrandSwitch, strandSwitchOrNot._1);

        // split between ref block switch or not (NOTE BOTH SAME CHROMOSOME MAPPING AND NO STRAND SWITCH)
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> tandemDupBkptOrSimpleInsDel =
                split(strandSwitchOrNot._2, AssemblyContigAlignmentSignatureClassifier::indicatesIntraChrTandemDupBkpts);
        contigsByRawTypes.put(RawTypes.InsDel, tandemDupBkptOrSimpleInsDel._2);

        final JavaRDD<AssemblyContigWithFineTunedAlignments> mappedInsertionBreakpointSuspects = diffChrBreakpoints.union(tandemDupBkptOrSimpleInsDel._1);
        contigsByRawTypes.put(RawTypes.MappedInsertionBkpt, mappedInsertionBreakpointSuspects);

        return contigsByRawTypes;
    }

    static boolean indicatesIntraChrStrandSwitchBkpts(final AssemblyContigWithFineTunedAlignments decoratedContig) {
        final AlignedContig contig = decoratedContig.getSourceContig();
        if (contig.alignmentIntervals.size() != 2)
            return false;

        final AlignmentInterval first = contig.alignmentIntervals.get(0);
        final AlignmentInterval last = contig.alignmentIntervals.get(1);
        if ( !first.referenceSpan.getContig().equals(last.referenceSpan.getContig()) )
            return false;

        return first.forwardStrand ^ last.forwardStrand;
    }

    static boolean indicatesIntraChrTandemDupBkpts(final AssemblyContigWithFineTunedAlignments decoratedContig) {
        final AlignedContig contig = decoratedContig.getSourceContig();
        if (contig.alignmentIntervals.size() != 2)
            return false;

        final AlignmentInterval first = contig.alignmentIntervals.get(0);
        final AlignmentInterval last = contig.alignmentIntervals.get(1);
        if ( !first.referenceSpan.getContig().equals(last.referenceSpan.getContig()) )
            return false;

        if (first.forwardStrand != last.forwardStrand)
            return false;

        return isCandidateSimpleTranslocation(first, last, StrandSwitch.NO_SWITCH);
    }

    /**
     * Determine if the chimeric alignment indicates a simple translocation.
     * Simple translocations are defined here and at this time as:
     * <ul>
     *  <li>inter-chromosomal translocations, i.e. novel adjacency between different reference chromosomes, or</li>
     *  <li>intra-chromosomal translocation that DOES NOT involve a strand switch, that is:
     *      novel adjacency between reference locations on the same chromosome involving reference-order switch but NO strand switch,
     *      but in the meantime, the two inducing alignments CANNOT overlap each other since that would point to
     *      incomplete picture, hence not "simple" anymore.
     *  </li>
     * </ul>
     * Note that the above definition specifically does not cover the case where
     * the CA suggests a novel adjacency between
     * two reference locations on the same chromosome involving a strand switch.
     * The reason is: to resolve/interpret such cases
     * (distinguish between a real inversion or dispersed inverted duplication),
     * we need other types of evidence in addition to contig alignment signatures.
     */
    @VisibleForTesting
    static boolean isCandidateSimpleTranslocation(final AlignmentInterval regionWithLowerCoordOnContig,
                                                  final AlignmentInterval regionWithHigherCoordOnContig,
                                                  final StrandSwitch strandSwitch) {

        if (!regionWithLowerCoordOnContig.referenceSpan.getContig()
                .equals(regionWithHigherCoordOnContig.referenceSpan.getContig()))
            return true;

        if ( !strandSwitch.equals(StrandSwitch.NO_SWITCH) )
            return false;

        final SimpleInterval referenceSpanOne = regionWithLowerCoordOnContig.referenceSpan,
                referenceSpanTwo = regionWithHigherCoordOnContig.referenceSpan;

        if (referenceSpanOne.contains(referenceSpanTwo) || referenceSpanTwo.contains(referenceSpanOne))
            return false;

        if (regionWithLowerCoordOnContig.forwardStrand) {
            return referenceSpanOne.getStart() > referenceSpanTwo.getEnd();
        } else {
            return referenceSpanTwo.getStart() > referenceSpanOne.getEnd();
        }
    }

    /**
     * todo : see ticket #3529 (Change to a more principled criterion than more than half of alignments overlapping)
     * @return true iff the two alignments of the assembly contig are
     *         1) mappings to different strands on the same chromosome, and
     *         2) overlapping on reference is more than half of the two AI's minimal read span.
     */
    @VisibleForTesting
    static boolean isCandidateInvertedDuplication(final AlignmentInterval one, final AlignmentInterval two) {
        if (one.forwardStrand == two.forwardStrand)
            return false;
        return 2 * AlignmentInterval.overlapOnRefSpan(one, two) > Math.min(one.getSizeOnRead(), two.getSizeOnRead());
    }

    /**
     * This predicate tests if an assembly contig has the full event (i.e. alt haplotype) assembled
     * Of course, the grand problem of SV is always not getting the big-enough picture
     * but here we have a more workable definition of what is definitely not big-enough:
     *
     * If the assembly contig, with its two (picked) alignments, shows any of the following signature,
     * it is definitely not giving the whole picture of the alt haplotype,
     * hence without other types of evidence (or linking breakpoints, which itself needs other evidence anyway),
     * human-friendly interpretation for them is unreliable.
     * <ul>
     *     <li>
     *         first and second alignment contain each other in terms of their reference span,
     *         regardless if strand switch is involved;
     *     </li>
     *     <li>
     *         first and second alignment involve strand switch, but their reference span overlap;
     *         todo: this obsoletes the actual use of {@link #isCandidateInvertedDuplication(AlignmentInterval, AlignmentInterval)}
     *               and related inverted duplication call code we have now,
     *               but those could be used to figure out how to annotate which known ref regions are invert duplicated
     *     </li>
     *     <li>
     *         first and second alignment have reference order switch but their reference span overlaps;
     *     </li>
     * </ul>
     */
    static boolean hasIncompletePictureFromTwoAlignments(final AlignmentInterval one, final AlignmentInterval two) {
        final SimpleInterval referenceSpanOne = one.referenceSpan;
        final SimpleInterval referenceSpanTwo = two.referenceSpan;

        // inter contig mapping will not be treated as incomplete picture for 2-alignment reads
        if ( ! referenceSpanOne.getContig().equals(referenceSpanTwo.getContig()) )
            return false;

        // ref span containment
        if (referenceSpanOne.contains(referenceSpanTwo) || referenceSpanTwo.contains(referenceSpanOne))
            return true;

        // overlapping strand-switch
        // TODO: 10/29/17 this obsoletes the inverted duplication call code we have now,
        //      but those could be used to figure out how to annotate which known ref regions are invert duplicated
        if (one.forwardStrand != two.forwardStrand &&
                referenceSpanOne.overlaps(referenceSpanTwo))
            return true;

        // no strand switch but overlapping reference order switch
        if (one.forwardStrand) {
            return referenceSpanOne.getStart() > referenceSpanTwo.getStart() &&
                    referenceSpanOne.getStart() <= referenceSpanTwo.getEnd();
        } else {
            return referenceSpanTwo.getStart() > referenceSpanOne.getStart() &&
                    referenceSpanTwo.getStart() <= referenceSpanOne.getEnd();
        }
    }

    //==================================================================================================================
    // FOR ASSEMBLY CONTIGS WITH MORE THAN TWO ALIGNMENTS

    /**
     * convenience struct holding 4 classes:
     *      1) contigs with 2 good alignments and bad alignments encoded as strings
     *      2) contigs with more than 2 good alignments and seemingly have picture complete as defined by
     *          {@link AssemblyContigWithFineTunedAlignments#hasIncompletePicture()}
     *      3) contigs with more than 2 good alignments but doesn't seem to have picture complete as defined by
     *          {@link AssemblyContigWithFineTunedAlignments#hasIncompletePicture()}
     *      4) non-informative contigs who after fine tuning has 0 or 1 good alignment left
     */
    static final class MultipleAlignmentReclassificationResults {
        final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithTwoGoodAlignments;
        final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithMoreThanTwoGoodAlignments;
        final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithMoreThanTwoGoodAlignmentsButIncompletePicture;
        final JavaRDD<AssemblyContigWithFineTunedAlignments> nonInformativeContigs;
        MultipleAlignmentReclassificationResults(final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithTwoGoodAlignments,
                                                 final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithMoreThanTwoGoodAlignments,
                                                 final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithMoreThanTwoGoodAlignmentsButIncompletePicture,
                                                 final JavaRDD<AssemblyContigWithFineTunedAlignments> nonInformativeContigs) {
            this.contigsWithTwoGoodAlignments = contigsWithTwoGoodAlignments;
            this.contigsWithMoreThanTwoGoodAlignments = contigsWithMoreThanTwoGoodAlignments;
            this.contigsWithMoreThanTwoGoodAlignmentsButIncompletePicture = contigsWithMoreThanTwoGoodAlignmentsButIncompletePicture;
            this.nonInformativeContigs = nonInformativeContigs;
        }
    }

    /**
     * Reclassify assembly contigs based on alignment fine tuning as implemented in
     * {@link #removeNonUniqueMappings(List, int, int)}.
     */
    static MultipleAlignmentReclassificationResults reClassifyContigsWithMultipleAlignments(
            final JavaRDD<AssemblyContigWithFineTunedAlignments> localAssemblyContigs,
            final int mapQThresholdInclusive, final int uniqReadLenInclusive) {

        final JavaRDD<AssemblyContigWithFineTunedAlignments> contigsWithFineTunedAlignments =
                localAssemblyContigs
                        .map(tig -> {
                            final AssemblyContigAlignmentsConfigPicker.GoodAndBadMappings refinedMappings =
                                    removeNonUniqueMappings(tig.getAlignments(), mapQThresholdInclusive, uniqReadLenInclusive);

                            final AlignedContig updatedTig = new AlignedContig(tig.getSourceContig().contigName, tig.getSourceContig().contigSequence,
                                    refinedMappings.getGoodMappings(), tig.hasEquallyGoodAlnConfigurations());
                            return new AssemblyContigWithFineTunedAlignments(updatedTig,
                                            refinedMappings.getBadMappingsAsCompactStrings(),
                                    tig.getSAtagForGoodMappingToNonCanonicalChromosome());
                        });

        // first take down non-informative assembly contigs
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> informativeAndNotSo =
                split(contigsWithFineTunedAlignments, AssemblyContigWithFineTunedAlignments::isInformative);
        final JavaRDD<AssemblyContigWithFineTunedAlignments> garbage = informativeAndNotSo._2;

        // assembly contigs with 2 good alignments and bad alignments encoded as strings
        final JavaRDD<AssemblyContigWithFineTunedAlignments> informativeContigs = informativeAndNotSo._1;
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> split =
                split(informativeContigs, AssemblyContigWithFineTunedAlignments::hasOnly2GoodAlignments);
        final JavaRDD<AssemblyContigWithFineTunedAlignments> twoGoodAlignments = split._1;

        // assembly contigs with more than 2 good alignments: without and with a complete picture
        final JavaRDD<AssemblyContigWithFineTunedAlignments> multipleAlignments = split._2;
        final Tuple2<JavaRDD<AssemblyContigWithFineTunedAlignments>, JavaRDD<AssemblyContigWithFineTunedAlignments>> split1 =
                split(multipleAlignments, AssemblyContigWithFineTunedAlignments::hasIncompletePicture);
        final JavaRDD<AssemblyContigWithFineTunedAlignments> multipleAlignmentsIncompletePicture = split1._1;
        final JavaRDD<AssemblyContigWithFineTunedAlignments> multipleAlignmentsCompletePicture = split1._2;

        return new MultipleAlignmentReclassificationResults(twoGoodAlignments, multipleAlignmentsCompletePicture,
                multipleAlignmentsIncompletePicture, garbage);
    }

    /**
     * Process provided {@code originalConfiguration} of an assembly contig and split between good and bad alignments.
     * The returned pair has good alignments as its first (i.e. updated configuration),
     * and bad ones as its second (could be used for, e.g. annotating mappings of inserted sequence).
     *
     * <p>
     *     What is considered good and bad?
     *     For a particular mapping/alignment, it may offer low uniqueness in two sense:
     *     <ul>
     *         <li>
     *             low REFERENCE UNIQUENESS: meaning the sequence being mapped match multiple locations on the reference;
     *         </li>
     *         <li>
     *             low READ UNIQUENESS: with only a very short part of the read uniquely explained by this particular alignment;
     *         </li>
     *     </ul>
     *     Good alignments offer both high reference uniqueness and read uniqueness, as judged by the requested
     *     {@code mapQThresholdInclusive} and {@code uniqReadLenInclusive}
     *     (yes we are doing a hard-filtering but more advanced model is not our priority right now 2017-11-20).
     * </p>
     *
     * Note that "original" is meant to be possibly different from the returned configuration,
     * but DOES NOT mean the alignments of the contig as given by the aligner,
     * i.e. the configuration should be one of the best given by
     * {@link AssemblyContigAlignmentsConfigPicker#pickBestConfigurations(AlignedContig, Set, Double)}.
     */
    static AssemblyContigAlignmentsConfigPicker.GoodAndBadMappings removeNonUniqueMappings(final List<AlignmentInterval> originalConfiguration,
                                                                                           final int mapQThresholdInclusive,
                                                                                           final int uniqReadLenInclusive) {
        Utils.validateArg(originalConfiguration.size() > 2,
                "assumption that input configuration to be fine tuned has more than 2 alignments is violated.\n" +
                        originalConfiguration.stream().map(AlignmentInterval::toPackedString).collect(Collectors.toList()));

        // two pass, each focusing on removing the alignments of a contig that offers low uniqueness in one sense:

        // first pass is for removing alignments with low REFERENCE UNIQUENESS, using low mapping quality as the criterion
        final List<AlignmentInterval> selectedAlignments = new ArrayList<>(originalConfiguration.size());
        final List<AlignmentInterval> lowUniquenessMappings = new ArrayList<>(originalConfiguration.size());

        for (final AlignmentInterval alignment : originalConfiguration) {
            if (alignment.mapQual >= mapQThresholdInclusive)
                selectedAlignments.add(alignment);
            else
                lowUniquenessMappings.add(alignment);
        }

        // second pass, the slower one, is to remove alignments offering low READ UNIQUENESS,
        // i.e. with only a very short part of the read being uniquely explained by this particular alignment;
        // the steps are:
        //      search bi-directionally until cannot find overlap any more,
        //      subtract the overlap from the distance covered on the contig by the alignment.
        //      This gives unique read region it explains.
        //      If this unique read region is "short": shorter than {@code uniqReadLenInclusive}), drop it.

        // each alignment has an entry of a tuple2, one for max overlap maxFront, one for max overlap maxRear,
        // max overlap maxFront is a tuple2 registering the index and overlap bases count
        final Map<AlignmentInterval, Tuple2<Integer, Integer>> maxOverlapMap = getMaxOverlapPairs(selectedAlignments);
        for(Iterator<AlignmentInterval> iterator = selectedAlignments.iterator(); iterator.hasNext();) {
            final AlignmentInterval alignment = iterator.next();

            final Tuple2<Integer, Integer> maxOverlapFrontAndRear = maxOverlapMap.get(alignment);
            final int maxOverlapFront = Math.max(0, maxOverlapFrontAndRear._1);
            final int maxOverlapRear = Math.max(0, maxOverlapFrontAndRear._2);

            // theoretically this could be negative for an alignment whose maxFront and maxRear sums together bigger than the read span
            // but earlier configuration scoring would make this impossible because such alignments should be filtered out already
            // considering that it brings more penalty than value, i.e. read bases explained (even if the MQ is 60),
            // but even if it is kept, a negative value won't hurt unless a stupid threshold value is passed in
            final int uniqReadSpan = alignment.endInAssembledContig - alignment.startInAssembledContig + 1
                                        - maxOverlapFront - maxOverlapRear;
            if (uniqReadSpan < uniqReadLenInclusive) {
                lowUniquenessMappings.add(alignment);
                iterator.remove();
            }
        }

        return new AssemblyContigAlignmentsConfigPicker.GoodAndBadMappings(selectedAlignments, lowUniquenessMappings);
    }

    /**
     * Each alignment in a specific configuration has an entry,
     * pointing to the alignments that comes before and after it,
     * that overlaps maximally (i.e. no other front or rear alignments have more overlaps)
     * with the current alignment.
     */
    private static final class TempMaxOverlapInfo {
        final Tuple2<Integer, Integer> maxFront; // 1st holds index pointing to another alignment before this, 2nd holds the count of overlapping bases
        final Tuple2<Integer, Integer> maxRear;  // same intention as above, but for alignments after this

        TempMaxOverlapInfo() {
            maxFront = new Tuple2<>(-1, -1);
            maxRear = new Tuple2<>(-1 ,-1);
        }

        TempMaxOverlapInfo(final Tuple2<Integer, Integer> maxFront, final Tuple2<Integer, Integer> maxRear) {
            this.maxFront = maxFront;
            this.maxRear = maxRear;
        }
    }

    /**
     * Extract the max overlap information, front and back, for each alignment in {@code configuration}.
     * For each alignment, the corresponding tuple2 has the max (front, rear) overlap base counts.
     */
    private static Map<AlignmentInterval, Tuple2<Integer, Integer>> getMaxOverlapPairs(final List<AlignmentInterval> configuration) {

        final List<TempMaxOverlapInfo> intermediateResult =
                new ArrayList<>(Collections.nCopies(configuration.size(), new TempMaxOverlapInfo()));

        // We iterate through all alignments except the last one
        // For the last alignment, which naturally doesn't have any maxRear,
        //     the following implementation sets its maxFront during the iteration, if available at all (it may overlap with nothing)
        for(int i = 0; i < configuration.size() - 1; ++i) {

            final AlignmentInterval cur = configuration.get(i);
            // For the i-th alignment, we only look at alignments after it (note j starts from i+1) and find max overlap
            int maxOverlapRearBases = -1;
            int maxOverlapRearIndex = -1;
            for (int j = i + 1; j < configuration.size(); ++j) { // note j > i
                final int overlap = AlignmentInterval.overlapOnContig(cur, configuration.get(j));
                if (overlap > maxOverlapRearBases) {
                    maxOverlapRearBases = overlap;
                    maxOverlapRearIndex = j;
                } else { // following ones, as guaranteed by the ordering of alignments in the contig, cannot overlap
                    break;
                }
            }

            if (maxOverlapRearBases > 0){
                // for current alignment (i-th), set its max_overlap_rear, which would not change in later iterations and copy old max_overlap_front
                final Tuple2<Integer, Integer> maxRear = new Tuple2<>(maxOverlapRearIndex, maxOverlapRearBases);
                final Tuple2<Integer, Integer> maxFrontToCopy = intermediateResult.get(i).maxFront;
                intermediateResult.set(i, new TempMaxOverlapInfo(maxFrontToCopy, maxRear));

                // then conditionally set the max_overlap_front of the
                // maxOverlapRearIndex-th alignment
                // that maximally overlaps with the current, i.e. i-th, alignment
                final TempMaxOverlapInfo oldValue = intermediateResult.get(maxOverlapRearIndex);// maxOverlapRearIndex cannot be -1 here
                if (oldValue.maxFront._2 < maxOverlapRearBases)
                    intermediateResult.set(maxOverlapRearIndex, new TempMaxOverlapInfo(new Tuple2<>(i, maxOverlapRearBases), oldValue.maxRear));
            }
        }

        final Map<AlignmentInterval, Tuple2<Integer, Integer>> maxOverlapMap = new HashMap<>(configuration.size());
        for (int i = 0; i < configuration.size(); ++i) {
            maxOverlapMap.put(configuration.get(i),
                    new Tuple2<>(intermediateResult.get(i).maxFront._2, intermediateResult.get(i).maxRear._2));
        }

        return maxOverlapMap;
    }


    //==================================================================================================================

    // TODO: 11/17/17 salvation on assembly contigs that 1) has ambiguous "best" configuration, and 2) has incomplete picture; and flag accordingly

    // TODO: 3/4/18 a bug is present here that even though only one alignment has not-bad MQ, it could contain a large gap,
    //      depending on the behavior of the other gap-less bad mappings,
    //      we may end up classifying the whole contig as incomplete, or signal-less,
    //      we should keep the single not-bad mapping and annotate accordingly
}
