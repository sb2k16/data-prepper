/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.configuration;


import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Objects;

public class StreamIdentifierValidator implements ConstraintValidator<ValidStreamIdentifier, KinesisStreamConfig> {

    @Override
    public boolean isValid(KinesisStreamConfig config, ConstraintValidatorContext context) {
        if (config == null) {
            return false;
        }

        return Objects.nonNull(config.getName()) ^ Objects.nonNull(config.getArn());
    }
}