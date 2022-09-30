package utils;

public class AwsCredentials {

    private String accessKey;
    private String secretAccessKey;

    public AwsCredentials(String[] credentials){
        this.accessKey = credentials[0];
        this.secretAccessKey = credentials[1];
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

}
