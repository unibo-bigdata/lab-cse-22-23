package utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Utils {

    /**
     * Get credentials from resources/aws_credentials.txt.
     *
     * File must contain:
     * ACCESS_KEY\n
     * SECRET_ACCESS_KEY\n
     * @return array with credentials
     */
    public static AwsCredentials credentialsFromFile() {

        String filename = "aws_credentials.txt";

        try (
            BufferedReader b = new BufferedReader(new InputStreamReader(
                    new Utils().getClass().getClassLoader().getResourceAsStream(filename)))
        ) {
            String readLine = "";
            final String[] credentials = new String[7];
            int i = 0;
            while ((readLine = b.readLine()) != null) {
                credentials[i++] = readLine;
            }
            b.close();

            AwsCredentials cred = new AwsCredentials(credentials);
            return cred;
        } catch (Exception e) {
            return null;
        }
    }
}
