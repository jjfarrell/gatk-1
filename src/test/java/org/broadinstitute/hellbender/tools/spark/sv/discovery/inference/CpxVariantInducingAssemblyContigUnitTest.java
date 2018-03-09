package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignedContig;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class CpxVariantInducingAssemblyContigUnitTest extends GATKBaseTest {

    @DataProvider(name = "forBasicInfoCtor")
    private Object[][] forBasicInfoCtor() {
        final List<Object[]> data = new ArrayList<>(20);

        return data.toArray(new Object[data.size()][]);
    }

    @Test(groups = "sv", dataProvider = "forBasicInfoCtor", enabled = false)
    public void testBasicInfoCtor(final AlignedContig contig,
                                  final CpxVariantInducingAssemblyContig.BasicInfo expectedResults) {
        final CpxVariantInducingAssemblyContig.BasicInfo basicInfo = new CpxVariantInducingAssemblyContig.BasicInfo(contig);
        Assert.assertEquals(basicInfo, expectedResults);
    }

    @DataProvider(name = "forJumpCtor")
    private Object[][] forJumpCtor() {
        final List<Object[]> data = new ArrayList<>(20);

        return data.toArray(new Object[data.size()][]);
    }

    @Test(groups = "sv", dataProvider = "forJumpCtor", enabled = false)
    public void testJumpCtor(final AlignmentInterval one, final AlignmentInterval two,
                             final CpxVariantInducingAssemblyContig.Jump expectedResults) {
        final CpxVariantInducingAssemblyContig.Jump jump = new CpxVariantInducingAssemblyContig.Jump(one, two);
        Assert.assertEquals(jump, expectedResults);
    }

    @DataProvider(name = "forExtractJumpsOnReference")
    private Object[][] forExtractJumpsOnReference() {
        final List<Object[]> data = new ArrayList<>(20);

        return data.toArray(new Object[data.size()][]);
    }

    @Test(groups = "sv", dataProvider = "forExtractJumpsOnReference", enabled = false)
    public void testExtractJumpsOnReference(final List<AlignmentInterval> alignments,
                                            final List<CpxVariantInducingAssemblyContig.Jump> expectedResults) {
        final List<CpxVariantInducingAssemblyContig.Jump> result = CpxVariantInducingAssemblyContig.extractJumpsOnReference(alignments);
        Assert.assertEquals(result, expectedResults);
    }

    @DataProvider(name = "forExtractSegmentingRefLocationsOnEventPrimaryChromosome")
    private Object[][] forExtractSegmentingRefLocationsOnEventPrimaryChromosome() {
        final List<Object[]> data = new ArrayList<>(20);

        return data.toArray(new Object[data.size()][]);
    }

    @Test(groups = "sv", dataProvider = "forExtractSegmentingRefLocationsOnEventPrimaryChromosome", enabled = false)
    public void testExtractSegmentingRefLocationsOnEventPrimaryChromosome(final List<CpxVariantInducingAssemblyContig.Jump> jumps,
                                                                          final CpxVariantInducingAssemblyContig.BasicInfo basicInfo,
                                                                          List<SimpleInterval> expectedResults) {
        final List<SimpleInterval> result =
                CpxVariantInducingAssemblyContig.extractSegmentingRefLocationsOnEventPrimaryChromosome(jumps, basicInfo,
                        CpxSVInferenceTestUtils.bareBoneHg38SAMSeqDict);
        Assert.assertEquals(result, expectedResults);
    }
}
