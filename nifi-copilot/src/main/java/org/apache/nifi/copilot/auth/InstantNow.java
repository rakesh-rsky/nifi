package org.apache.nifi.copilot.auth;

final class InstantNow {
    private InstantNow() {
    }

    static long epochSec() {
        return System.currentTimeMillis() / 1000L;
    }
}
