package org.broadinstitute.hellbender.tools.copynumber.utils.annotatedregion;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.reference.ReferenceUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class AnnotatedIntervalCollectionUnitTest extends GATKBaseTest {

    private static final File TEST_FILE = new File(toolsTestDir,
            "copynumber/utils/annotated-interval-many-columns.tsv");
    private static final File TEST_SAM_COMMENTS_FILE = new File(toolsTestDir,
            "copynumber/utils/annotated-interval-alt-samheader.tsv");
    private static final File TEST_CONFIG = new File(toolsTestDir,
            "copynumber/utils/test.config");
    private static final File TEST_NAMED_CONFIG = new File(toolsTestDir,
            "copynumber/utils/test_col_names.config");


    @Test
    public void basicTest() throws IOException {
        final Set<String> headersOfInterest = Sets.newHashSet(Arrays.asList("name", "learning_SAMPLE_0"));
        final AnnotatedIntervalCollection simpleAnnotatedGenomicRegions =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), headersOfInterest);

        Assert.assertEquals(simpleAnnotatedGenomicRegions.size(), 15);
        Assert.assertTrue(simpleAnnotatedGenomicRegions.getRecords().stream()
                .mapToInt(s -> s.getAnnotations().entrySet().size())
                .allMatch(i -> i == headersOfInterest.size()));
        Assert.assertTrue(simpleAnnotatedGenomicRegions.getRecords().stream().allMatch(s -> s.getAnnotations().keySet().containsAll(headersOfInterest)));

        // Grab the first 15 and test values
        List<AnnotatedInterval> gtRegions = Arrays.asList(
                new AnnotatedInterval(new SimpleInterval("1", 30365, 30503), ImmutableSortedMap.of("name", "target_1_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 69088, 70010), ImmutableSortedMap.of("name", "target_2_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 367656, 368599), ImmutableSortedMap.of("name", "target_3_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 621093, 622036), ImmutableSortedMap.of("name", "target_4_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 861319, 861395), ImmutableSortedMap.of("name", "target_5_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 865532, 865718), ImmutableSortedMap.of("name", "target_6_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 866416, 866471), ImmutableSortedMap.of("name", "target_7_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 871149, 871278), ImmutableSortedMap.of("name", "target_8_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 874417, 874511), ImmutableSortedMap.of("name", "target_9_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 874652, 874842), ImmutableSortedMap.of("name", "target_10_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 876521, 876688), ImmutableSortedMap.of("name", "target_11_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 877513, 877633), ImmutableSortedMap.of("name", "target_12_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 877787, 877870), ImmutableSortedMap.of("name", "target_13_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 877936, 878440), ImmutableSortedMap.of("name", "target_14_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 878630, 878759), ImmutableSortedMap.of("name", "target_15_SAMD11", "learning_SAMPLE_0", "2"))
        );

        Assert.assertEquals(simpleAnnotatedGenomicRegions.getRecords().subList(0, gtRegions.size()), gtRegions);
    }

    @Test
    public void testCreationFromList() {
        final Set<String> headersOfInterest = Sets.newHashSet(Arrays.asList("name", "learning_SAMPLE_0"));
        final List<AnnotatedInterval> annotatedIntervals =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), headersOfInterest).getRecords();
        final AnnotatedIntervalCollection collection = AnnotatedIntervalCollection.create(annotatedIntervals,
                new SAMFileHeader(ReferenceUtils.loadFastaDictionary(new File(hg19_chr1_1M_dict))),
                Lists.newArrayList("name", "learning_SAMPLE_0"));

        Assert.assertEquals(collection.getRecords(), annotatedIntervals);
    }

    @Test
    public void basicTestWithAllColumnsFile() throws IOException {

        // If no columns of interest are given in a read call, the method will try to load all columns as "interesting".
        final AnnotatedIntervalCollection simpleAnnotatedGenomicRegions =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), null);

        Assert.assertEquals(simpleAnnotatedGenomicRegions.getRecords().size(), 15);
        Assert.assertTrue(simpleAnnotatedGenomicRegions.getRecords().stream()
                .mapToInt(s -> s.getAnnotations().entrySet().size())
                .allMatch(i -> i == 101)); // The number of columns in the TEST_FILE (name, learning_SAMPLE_0...99
    }

    @Test
    public void basicTestTribble() throws IOException {
        final AnnotatedIntervalCollection simpleAnnotatedGenomicRegions =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), TEST_CONFIG.toPath(), null);

        Assert.assertEquals(simpleAnnotatedGenomicRegions.getRecords().size(), 15);
        Assert.assertTrue(simpleAnnotatedGenomicRegions.getRecords().stream()
                .allMatch(s -> s.getAnnotations().entrySet().size() == 101));
    }

    @Test
    public void basicTestTribbleWithHeadersOfInterest() throws IOException {
        final Set<String> headersOfInterest = Sets.newHashSet(Arrays.asList("name", "learning_SAMPLE_0"));
        final AnnotatedIntervalCollection simpleAnnotatedGenomicRegions =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), TEST_CONFIG.toPath(), headersOfInterest);

        assertLearningSampleTest(headersOfInterest, simpleAnnotatedGenomicRegions);
    }

    private void assertLearningSampleTest(Set<String> headersOfInterest, AnnotatedIntervalCollection simpleAnnotatedGenomicRegions) {
        Assert.assertEquals(simpleAnnotatedGenomicRegions.size(), 15);
        Assert.assertTrue(simpleAnnotatedGenomicRegions.getRecords().stream()
                .allMatch(s -> s.getAnnotations().entrySet().size() == 2));
        Assert.assertTrue(simpleAnnotatedGenomicRegions.getRecords().stream().allMatch(s -> s.getAnnotations().keySet().containsAll(headersOfInterest)));

        // Grab the first 15 and test values
        List<AnnotatedInterval> gtRegions = Arrays.asList(
                new AnnotatedInterval(new SimpleInterval("1", 30365, 30503), ImmutableSortedMap.of("name", "target_1_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 69088, 70010), ImmutableSortedMap.of("name", "target_2_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 367656, 368599), ImmutableSortedMap.of("name", "target_3_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 621093, 622036), ImmutableSortedMap.of("name", "target_4_None", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 861319, 861395), ImmutableSortedMap.of("name", "target_5_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 865532, 865718), ImmutableSortedMap.of("name", "target_6_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 866416, 866471), ImmutableSortedMap.of("name", "target_7_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 871149, 871278), ImmutableSortedMap.of("name", "target_8_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 874417, 874511), ImmutableSortedMap.of("name", "target_9_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 874652, 874842), ImmutableSortedMap.of("name", "target_10_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 876521, 876688), ImmutableSortedMap.of("name", "target_11_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 877513, 877633), ImmutableSortedMap.of("name", "target_12_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 877787, 877870), ImmutableSortedMap.of("name", "target_13_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 877936, 878440), ImmutableSortedMap.of("name", "target_14_SAMD11", "learning_SAMPLE_0", "2")),
                new AnnotatedInterval(new SimpleInterval("1", 878630, 878759), ImmutableSortedMap.of("name", "target_15_SAMD11", "learning_SAMPLE_0", "2"))
        );

        Assert.assertEquals(simpleAnnotatedGenomicRegions.getRecords().subList(0, gtRegions.size()), gtRegions);
    }

    @Test
    public void basicTestTribbleWithHeadersOfInterestColumnNamesSpecified() throws IOException {
        final Set<String> headersOfInterest = Sets.newHashSet(Arrays.asList("name", "learning_SAMPLE_0"));
        final AnnotatedIntervalCollection simpleAnnotatedGenomicRegions =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), TEST_NAMED_CONFIG.toPath(), headersOfInterest);

        assertLearningSampleTest(headersOfInterest, simpleAnnotatedGenomicRegions);
    }

    @Test
    public void basicTestTribbleWithNoConfig() throws IOException {
        final Set<String> headersOfInterest = Sets.newHashSet(Arrays.asList("name", "learning_SAMPLE_0"));
        final AnnotatedIntervalCollection simpleAnnotatedGenomicRegions =
                AnnotatedIntervalCollection.create(TEST_FILE.toPath(), headersOfInterest);

        assertLearningSampleTest(headersOfInterest, simpleAnnotatedGenomicRegions);
    }

    @Test()
    public void testCreateSamFileHeaderComments() {
        final AnnotatedIntervalCollection collection = AnnotatedIntervalCollection.create(TEST_SAM_COMMENTS_FILE.toPath(), null);
        Assert.assertEquals(collection.getComments(), Arrays.asList("foo", "foo2 baz"));
    }

    @Test()
    public void testCreateSamFileHeaderWithCommentsOnly() {
        final SAMFileHeader emptyHeader = AnnotatedIntervalCollection.createSamFileHeader(Collections.emptyList());
        final SAMFileHeader header = AnnotatedIntervalCollection.createSamFileHeaderWithCommentsOnly(Arrays.asList("This is a comment"));
        Assert.assertEquals(header.getComments(), Arrays.asList("@CO\tThis is a comment"));
        Assert.assertEquals(header.getReadGroups(), emptyHeader.getReadGroups());
        Assert.assertEquals(header.getSequenceDictionary(), emptyHeader.getSequenceDictionary());
        Assert.assertEquals(header.getSortOrder(), emptyHeader.getSortOrder());
    }
}
