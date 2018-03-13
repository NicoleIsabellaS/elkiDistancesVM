package elkiExample;

import java.io.IOException;

public class VM_3 {

	public static void main(String[] args) {
		try {
			// 66732-100097 of 100097
			System.out.println("Please make sure that your output folder is empty!");
			Calculation.start(3);
		} catch (IOException e) {
			System.out.println("VM terminated. Try again! " + e);
		}
	}
}
