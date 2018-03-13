package elkiExample;

import java.io.IOException;

public class VM_1 {

	public static void main(String[] args) throws IOException {
		try {
			// 1-33365 of 100.097 --> 1 bis 50048
			System.out.println("Please make sure that your output folder is empty!");
			Calculation.start(1);
		} catch (IOException e) {
			System.out.println("VM terminated. Try again! " + e);
		}
	}
}
