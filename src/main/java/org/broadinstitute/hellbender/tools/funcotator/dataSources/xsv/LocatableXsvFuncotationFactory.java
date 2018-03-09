package org.broadinstitute.hellbender.tools.funcotator.dataSources.xsv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.tribble.Feature;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.funcotator.DataSourceFuncotationFactory;
import org.broadinstitute.hellbender.tools.funcotator.Funcotation;
import org.broadinstitute.hellbender.tools.funcotator.FuncotatorArgumentDefinitions;
import org.broadinstitute.hellbender.tools.funcotator.dataSources.TableFuncotation;
import org.broadinstitute.hellbender.tools.funcotator.dataSources.gencode.GencodeFuncotation;
import org.broadinstitute.hellbender.utils.codecs.xsvLocatableTable.XsvLocatableTableCodec;
import org.broadinstitute.hellbender.utils.codecs.xsvLocatableTable.XsvTableFeature;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 *  Factory for creating {@link TableFuncotation}s by handling `Separated Value` files with arbitrary delimiters
 * (e.g. CSV/TSV files) which contain data that are locatable (i.e. {@link org.broadinstitute.hellbender.utils.codecs.xsvLocatableTable.XsvTableFeature}).
 *
 * This is a high-level object that interfaces with the internals of {@link org.broadinstitute.hellbender.tools.funcotator.Funcotator}.
 * Created by jonn on 12/6/17.
 */
public class LocatableXsvFuncotationFactory extends DataSourceFuncotationFactory {

    //==================================================================================================================
    // Public Static Members:

    //==================================================================================================================
    // Private Static Members:

    @VisibleForTesting
    static String DEFAULT_NAME = "LocatableXsv";

    //==================================================================================================================
    // Private Members:

    /**
     * Name of this {@link LocatableXsvFuncotationFactory}.
     */
    private final String name;

    /**
     * Names of all fields supported by this {@link LocatableXsvFuncotationFactory}.
     */
    private LinkedHashSet<String> supportedFieldNames = null;

    //==================================================================================================================
    // Constructors:

    public LocatableXsvFuncotationFactory(){
        this(DEFAULT_NAME);
    }

    public LocatableXsvFuncotationFactory(final String name){
        this.name = name;
    }

    //==================================================================================================================
    // Override Methods:

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LinkedHashSet<String> getSupportedFuncotationFields() {

        if ( supportedFieldNames == null ) {
            throw new GATKException("Must set supportedFuncotationFields before querying for them!");
        }
        else {
            return new LinkedHashSet<>(supportedFieldNames);
        }
    }

    @Override
    public List<Funcotation> createFuncotations(final VariantContext variant, final ReferenceContext referenceContext, final List<Feature> featureList) {
        final List<Funcotation> outputFuncotations = new ArrayList<>();

        if ( !featureList.isEmpty() ) {
            for ( final Feature feature : featureList ) {
                // Get the kind of feature we want here:
                if ( (feature != null) && XsvTableFeature.class.isAssignableFrom(feature.getClass()) ) {
                    outputFuncotations.add( new TableFuncotation((XsvTableFeature) feature) );
                }
            }
        }

        return outputFuncotations;
    }

    @Override
    public List<Funcotation> createFuncotations(final VariantContext variant, final ReferenceContext referenceContext, final List<Feature> featureList, final List<GencodeFuncotation> gencodeFuncotations) {
        return createFuncotations(variant, referenceContext, featureList);
    }

    @Override
    public FuncotatorArgumentDefinitions.DataSourceType getType() {
        return FuncotatorArgumentDefinitions.DataSourceType.LOCATABLE_XSV;
    }

    //==================================================================================================================
    // Static Methods:

    //==================================================================================================================
    // Instance Methods:

    public void setSupportedFuncotationFields(final List<Path> inputDataFilePaths) {

        // Approximate starting size:
        supportedFieldNames = new LinkedHashSet<>(inputDataFilePaths.size() * 10);

        for ( final Path dataPath : inputDataFilePaths ) {

            final XsvLocatableTableCodec codec = new XsvLocatableTableCodec();
            List<String> header = null;

            if (codec.canDecode(dataPath.toString())) {
                try (final InputStream fileInputStream = Files.newInputStream(dataPath)) {

                    // Lots of scaffolding to do reading here:
                    final AsciiLineReaderIterator lineReaderIterator = new AsciiLineReaderIterator(AsciiLineReader.from(fileInputStream));
                    codec.readActualHeader(lineReaderIterator);
                    header = codec.getHeaderWithoutLocationColumns();

                } catch (final IOException ioe) {
                    throw new UserException.BadInput("Could not read header from data file: " + dataPath.toUri().toString(), ioe);
                }
            }

            // Make sure we actually read the header:
            if ( header == null ) {
                throw new UserException.MalformedFile("Could not decode from data file: " + dataPath.toUri().toString());
            }

            // Add the header fields to the supportedFieldNames:
            supportedFieldNames.addAll(header);
        }
    }

    //==================================================================================================================
    // Helper Data Types:

}
