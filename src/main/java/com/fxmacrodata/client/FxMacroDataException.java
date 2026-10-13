package com.fxmacrodata.client;

/** A public error that never includes a credential or request URL. */
public final class FxMacroDataException extends RuntimeException {
    public FxMacroDataException(String message) {
        super(message, null, false, true);
    }
}
