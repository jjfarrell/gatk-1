package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignmentInterval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class CpxVariantInterpreterUnitTest extends GATKBaseTest {

    @DataProvider(name = "forTestDeOverlapAlignments")
    private Object[][] forTestDeOverlapAlignments() {
        final List<Object[]> data = new ArrayList<>(20);

        return data.toArray(new Object[data.size()][]);
    }

    @Test(groups = "sv", dataProvider = "forTestDeOverlapAlignments")
    public void testDeOverlapAlignments(final List<AlignmentInterval> alignments,
                                        final List<AlignmentInterval> expectedResults) {
        final List<AlignmentInterval> result = CpxVariantInterpreter.deOverlapAlignments(alignments,
                CpxSVInferenceTestUtils.bareBoneHg38SAMSeqDict);
        Assert.assertEquals(result, expectedResults);
    }
}
