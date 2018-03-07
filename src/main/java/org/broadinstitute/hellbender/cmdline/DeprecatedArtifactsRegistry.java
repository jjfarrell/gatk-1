package org.broadinstitute.hellbender.cmdline;

import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * When a tool is removed from GATK (after having been @Deprecated for a suitable period), an entry should
 * be added to this list to issue a message when the user tries to run that tool.
 *
 * NOTE: Picard tools should be listed here as well, since by definition such tools will not be found in
 * the Picard jar.
 */
public class DeprecatedArtifactsRegistry {

    // Mapping from tool name to string describing the major version number where the tool first disappeared and
    // optional recommended alternatives
    private static Map<String, Tuple2<String, String>> deprecatedTools = new HashMap<>();

    static {
        // Indicate version in which the tool disappeared, and recommended replacement in parentheses if applicable
        deprecatedTools.put("IndelRealigner", new Tuple2<>("4.0.0.0", "Please use GATK3 to run this tool"));
        deprecatedTools.put("RealignerTargetCreator", new Tuple2<>("4.0.0.0", "Please use GATK3 to run this tool"));
    }

    // Mapping from walker name to major version number where the walker first disappeared and optional replacement options
    private static Map<String, String> deprecatedAnnotations = new HashMap<>();
    static {
        // Indicate version in which the annotation disappeared, and recommended replacement in parentheses if applicable
    }

    /**
     * Utility method to pull up the version number at which a tool was deprecated and the suggested replacement, if any
     *
     * @param toolName   the tool class name (not the full package) to check
     */
    public static String getToolDeprecationInfo(final String toolName) {
        return deprecatedTools.containsKey(toolName) ?
                String.format("%s is no longer included in GATK as of version %s. (%s)",
                        toolName,
                        deprecatedTools.get(toolName)._1,
                        deprecatedTools.get(toolName)._2
                ) :
                null;
    }

    /**
     * Utility method to pull up the version number at which an annotation was deprecated and the suggested replacement, if any
     *
     * @param annotationName   the annotation class name (not the full package) to check
     */
    public static String getAnnotationDeprecationInfo(final String annotationName) {
        return deprecatedAnnotations.get(annotationName).toString();
    }

}
