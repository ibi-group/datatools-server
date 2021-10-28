package com.conveyal.datatools.manager.extensions.mtc;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class MtcFeedResourceTest {
    @Test
    void shouldConvertRtdNullToEmptyString() {
        assertThat(MtcFeedResource.convertRtdString("null"), equalTo(""));
        assertThat(MtcFeedResource.convertRtdString("Other text"), equalTo("Other text"));
    }
}
