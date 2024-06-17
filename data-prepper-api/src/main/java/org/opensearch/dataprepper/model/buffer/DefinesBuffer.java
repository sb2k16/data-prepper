package org.opensearch.dataprepper.model.buffer;

import java.util.Optional;

public interface DefinesBuffer {

    Optional<Buffer> getDefaultBuffer();
}
