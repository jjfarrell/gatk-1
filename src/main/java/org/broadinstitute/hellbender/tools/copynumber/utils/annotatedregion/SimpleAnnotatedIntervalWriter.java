package org.broadinstitute.hellbender.tools.copynumber.utils.annotatedregion;

import com.google.common.collect.Lists;
import htsjdk.samtools.SAMFileHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;
import org.broadinstitute.hellbender.utils.tsv.TableWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Callers must call {@link #writeHeader} before {@link #add}.
 *
 * This class is not thread-safe.
 */
public class SimpleAnnotatedIntervalWriter implements AnnotatedIntervalWriter {

    public static final String CONTIG_COL_COMMENT = "_ContigHeader=";
    public static final String START_COL_COMMENT = "_StartHeader=";
    public static final String END_COL_COMMENT = "_EndHeader=";
    private SimpleTableWriter writer;
    private FileWriter fileWriter;
    private String contigColumnHeader;
    private String startColumnHeader;
    private String endColumnHeader;
    private File outputFile;
    private boolean hasHeaderBeenWritten = false;
    private static final Logger logger = LogManager.getLogger(SimpleAnnotatedIntervalWriter.class);

    private class SimpleTableWriter extends TableWriter<AnnotatedInterval> {

        SimpleTableWriter(final Writer writer, TableColumnCollection tableColumns) throws IOException {
            super(writer, tableColumns);
        }

        @Override
        protected void composeLine(final AnnotatedInterval record, final DataLine dataLine) {
            // First the Locatable info
            dataLine.set(contigColumnHeader, record.getContig());
            dataLine.set(startColumnHeader, record.getStart());
            dataLine.set(endColumnHeader, record.getEnd());

            // Now everything else.
            record.getAnnotations().keySet().forEach(k -> dataLine.set(k, record.getAnnotationValue(k)));
        }

    }

    /**
     * Initialize this writer to the given output file.
     *
     * @param outputFile destination file.  Must be writeable.
     */
    public SimpleAnnotatedIntervalWriter(final File outputFile) {
        Files.isWritable(outputFile.toPath());
        this.outputFile = outputFile;
    }

    private void initializeForWriting(final String contigColumnName, final String startColumnName, final String endColumnName, final List<String> annotations) {
        final List<String> finalColumnList = Lists.newArrayList(contigColumnName, startColumnName, endColumnName);
        finalColumnList.addAll(annotations);
        try {
            fileWriter = new FileWriter(outputFile);

            // By initializing writer to be based on fileWriter, writer.close will close the fileWriter as well.
            writer = new SimpleTableWriter(fileWriter, new TableColumnCollection(finalColumnList));
        } catch (final IOException ioe) {
            throw new GATKException("Could not create: " + outputFile.getAbsolutePath(), ioe);
        }

        this.contigColumnHeader = contigColumnName;
        this.startColumnHeader = startColumnName;
        this.endColumnHeader = endColumnName;
    }

    @Override
    public void writeHeader(final AnnotatedIntervalHeader annotatedIntervalHeader) {
        if (!hasHeaderBeenWritten) {
            initializeForWriting(annotatedIntervalHeader.getContigColumnName(), annotatedIntervalHeader.getStartColumnName(), annotatedIntervalHeader.getEndColumnName(), annotatedIntervalHeader.getAnnotations());
            try {

                // Fold the comments into the SAM file header.
                // Remove old structured comments, if present.
                final List<String> commentsToWrite = annotatedIntervalHeader.getSamFileHeader().getComments().stream()
                        .filter(c -> !c.startsWith(CONTIG_COL_COMMENT))
                        .filter(c -> !c.startsWith(START_COL_COMMENT))
                        .filter(c -> !c.startsWith(END_COL_COMMENT)).collect(Collectors.toList());

                // Write out the column headers as a comment
                commentsToWrite.add(CONTIG_COL_COMMENT + annotatedIntervalHeader.getContigColumnName());
                commentsToWrite.add(START_COL_COMMENT + annotatedIntervalHeader.getStartColumnName());
                commentsToWrite.add(END_COL_COMMENT + annotatedIntervalHeader.getEndColumnName());

                // A bit more manual to write the SAM Header
                if (annotatedIntervalHeader.getSamFileHeader() != null) {
                    final SAMFileHeader finalSamHeader = annotatedIntervalHeader.getSamFileHeader().clone();
                    finalSamHeader.setComments(commentsToWrite);
                    fileWriter.write(finalSamHeader.getSAMString());
                }
                writer.writeHeaderIfApplies();
            } catch (final IOException e) {
                throw new UserException.CouldNotCreateOutputFile("Could not write to file.", e);
            }

            hasHeaderBeenWritten = true;
        } else {
            logger.warn("Attempted to write header twice.  Ignoring this request.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            // Only close the SimpleTableWriter, since it will close fileWriter
            writer.close();
        } catch (final IOException e) {
            throw new UserException.CouldNotCreateOutputFile("Could not close file writing.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(final AnnotatedInterval annotatedInterval) {
        if (!hasHeaderBeenWritten) {
            throw new GATKException.ShouldNeverReachHereException("This is an invalid code path, since the header of the output file should already be written.  Please post this error to the GATK forum (https://gatkforums.broadinstitute.org/gatk)");
        }
        try {
            writer.writeRecord(annotatedInterval);
        } catch (IOException e) {
            throw new UserException.CouldNotCreateOutputFile("Could not write to file.", e);
        }
    }
}
