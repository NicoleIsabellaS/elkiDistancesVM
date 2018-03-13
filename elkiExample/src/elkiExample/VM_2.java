package elkiExample;

import java.io.IOException;

public class VM_2 {

	public static void main(String[] args) {
		try {
			// 3336-66731 of 100097 --> 50049 bis 100097
			System.out.println("Please make sure that your output folder is empty!");
			Calculation.start(2);
		} catch (IOException e) {
			System.out.println("VM terminated. Try again! " + e);
		}
	}
}
