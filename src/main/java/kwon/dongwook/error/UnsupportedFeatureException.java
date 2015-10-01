package kwon.dongwook.error;



public class UnsupportedFeatureException extends Exception {

    public static final int UNSUPPORTED_QUERY = 100;

    private int reasonCode = 0;
    public int getReasonCode() {
        return reasonCode;
    }

    public UnsupportedFeatureException(String message) {
        super(message);
    }

    public UnsupportedFeatureException(String message, int reasonCode) {
        this(message);
        this.reasonCode = reasonCode;
    }

}
