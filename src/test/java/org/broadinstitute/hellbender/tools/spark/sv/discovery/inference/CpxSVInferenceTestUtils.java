package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.io.IOUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * For providing test data and utils specifically for complex sv.
 * Any attempts to reuse these resources elsewhere,
 * even for simple SV's
 * (see {@link org.broadinstitute.hellbender.tools.spark.sv.discovery.SimpleSVDiscoveryTestDataProvider} for that purpose)
 * should be take extreme care.
 */
class CpxSVInferenceTestUtils {




    /**
     * We are having this because it is SV, especially complex ones, are rare and events on chr20 and 21 are not enough.
     */
    final static SAMSequenceDictionary bareBoneHg38SAMSeqDict;
    static {
        final List<SAMSequenceRecord> hg38Chromosomes = new ArrayList<>();
        final String hg38ChrBareBoneListFile =  GATKBaseTest.toolsTestDir + "/spark/sv/utils/hg38ChrBareBone.txt";
        try (final Stream<String> records = Files.lines(IOUtils.getPath(( hg38ChrBareBoneListFile )))) {
            records.forEach(line -> {
                final String[] fields = line.split("\t", 2);
                hg38Chromosomes.add(new SAMSequenceRecord(fields[0], Integer.valueOf(fields[1])));
            });
            bareBoneHg38SAMSeqDict = new SAMSequenceDictionary(hg38Chromosomes);
        } catch ( final IOException ioe ) {
            throw new UserException("Can't read nonCanonicalContigNamesFile file " + hg38ChrBareBoneListFile, ioe);
        }
    }
}
