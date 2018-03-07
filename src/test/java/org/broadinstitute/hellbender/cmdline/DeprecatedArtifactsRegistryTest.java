package org.broadinstitute.hellbender.cmdline;

import org.broadinstitute.hellbender.Main;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeprecatedArtifactsRegistryTest {

    @Test
    public void testRunMissingDeprecatedTool() {
        final String missingTool = "IndelRealigner";

        final UserException e = Assert.expectThrows(
                UserException.class,
                () -> new Main().instanceMain( new String[] {missingTool} )
        );
        Assert.assertTrue(e.getMessage().contains(DeprecatedArtifactsRegistry.getToolDeprecationInfo(missingTool)));
    }

}
