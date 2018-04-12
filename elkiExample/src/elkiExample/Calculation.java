package elkiExample;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import de.lmu.ifi.dbs.elki.distance.distancefunction.CosineDistanceFunction;
import de.lmu.ifi.dbs.elki.distance.distancefunction.minkowski.EuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.distance.distancefunction.minkowski.ManhattanDistanceFunction;
import de.lmu.ifi.dbs.elki.math.linearalgebra.Vector;

/**
 * Computes three different distance functions (cosine, eucliden, manhattan)
 * form one csv file. The class is used with three VMs which separately will
 * calculate the distances of the various patient vectors created in parallel.
 */
public class Calculation {

	static String path = "/home/nicole.sarna/elkiDistancesVM/elkiExample/src/elkiExample/data.csv";
	static Map<Integer, String> admissionPatientMap = new HashMap<>();

	/**
	 * Invokes vector creation and distance calculation of a given path written by
	 * the user in console.
	 * 
	 * @param vmID
	 *            checks VM's ID to match appropriate calculation part
	 * @throws IOException
	 *             if a failed or interrupted I/O operation occurs for the output
	 *             files
	 */
	public static void start(int vmID) throws IOException {
		Map<Integer, Vector> patientsVectors = vectorizePatients(vmID);
		chunkingCalc(patientsVectors, vmID);
		// calcDistance(patientsVectors, vmID);
	}

	/**
	 * Vectorizes the patient data of the input file containing ~42 columns. It
	 * treats patients occurring twice as new vectors.
	 * 
	 * @return Map with unique admissionID from the dataset and the patient vector
	 *         created
	 * @throws IOException
	 *             if a failed or interrupted I/O operation occurs reading the input
	 *             file
	 */
	public static Map<Integer, Vector> vectorizePatients(int vmID) throws IOException {
		// String path = "data/diabetes.txt";
		Reader reader = Files.newBufferedReader(Paths.get(path));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
		Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);

		// Vector Size 129
		double[] values = new double[129];
		Vector userVector = null;
		Map<Integer, Vector> vectors = new HashMap<>();
		int rowNumber = 1;

		System.out.println("Step 1 \t CREATE VECTORS");
		for (CSVRecord csvRecord : csvRecords) {
			String gender = csvRecord.get(2);
			setBinaryVec(gender, values, 0);

			String age = csvRecord.get(3);
			values[2] = Integer.parseInt(age);

			String admission_type_id = csvRecord.get(4);
			setDistinctVecFor(admission_type_id, values, 3, 9);

			// discharge_disposition_id
			setDistinctVecFor(csvRecord.get(5), values, 12, 29);

			// admission_source_id
			setDistinctVecFor(csvRecord.get(6), values, 41, 21);

			String time_in_hospital = csvRecord.get(7);
			values[62] = Integer.parseInt(time_in_hospital);

			String num_lab_procedures = csvRecord.get(8);
			values[63] = Integer.parseInt(num_lab_procedures);

			String num_procedures = csvRecord.get(9);
			values[64] = Integer.parseInt(num_procedures);

			String num_medications = csvRecord.get(10);
			values[65] = Integer.parseInt(num_medications);

			String num_outpatient = csvRecord.get(11);
			values[66] = Integer.parseInt(num_outpatient);

			String num_emergency = csvRecord.get(12);
			values[67] = Integer.parseInt(num_emergency);

			String num_inpatient = csvRecord.get(13);
			values[68] = Integer.parseInt(num_inpatient);

			String diag_1 = csvRecord.get(14);
			values[69] = Double.parseDouble(diag_1);

			String num_diagnoses = csvRecord.get(15);
			values[70] = Integer.parseInt(num_diagnoses);

			String max_glu_serum = csvRecord.get(16);
			setTripleVec(max_glu_serum, values, 71);

			String A1Cresult = csvRecord.get(17);
			setTripleVec(A1Cresult, values, 74);

			String metformin = csvRecord.get(18);
			setBinaryVec(metformin, values, 77);

			String repaglinide = csvRecord.get(19);
			setBinaryVec(repaglinide, values, 79);

			String nateglinide = csvRecord.get(20);
			setBinaryVec(nateglinide, values, 81);

			String chlorpropamide = csvRecord.get(21);
			setBinaryVec(chlorpropamide, values, 83);

			String glimepiride = csvRecord.get(22);
			setBinaryVec(glimepiride, values, 85);

			String acetohexamide = csvRecord.get(23);
			setBinaryVec(acetohexamide, values, 87);

			String glipizide = csvRecord.get(24);
			setBinaryVec(glipizide, values, 89);

			String glyburide = csvRecord.get(25);
			setBinaryVec(glyburide, values, 91);

			String tolbutamide = csvRecord.get(26);
			setBinaryVec(tolbutamide, values, 93);

			String pioglitazone = csvRecord.get(27);
			setBinaryVec(pioglitazone, values, 95);

			String rosiglitazone = csvRecord.get(28);
			setBinaryVec(rosiglitazone, values, 97);

			String acarbose = csvRecord.get(29);
			setBinaryVec(acarbose, values, 99);

			String miglitol = csvRecord.get(30);
			setBinaryVec(miglitol, values, 101);

			String troglitazone = csvRecord.get(31);
			setBinaryVec(troglitazone, values, 103);

			String tolazamide = csvRecord.get(32);
			setBinaryVec(tolazamide, values, 105);

			String examide = csvRecord.get(33);
			setBinaryVec(examide, values, 107);

			String citoglipton = csvRecord.get(34);
			setBinaryVec(citoglipton, values, 109);

			String insulin = csvRecord.get(35);
			setBinaryVec(insulin, values, 111);

			String glyburide_metformin = csvRecord.get(36);
			setBinaryVec(glyburide_metformin, values, 113);

			String glipizide_metformin = csvRecord.get(37);
			setBinaryVec(glipizide_metformin, values, 115);

			String glimepiride_pioglitazone = csvRecord.get(38);
			setBinaryVec(glimepiride_pioglitazone, values, 117);

			String metformin_rosiglitazone = csvRecord.get(39);
			setBinaryVec(metformin_rosiglitazone, values, 119);

			String metformin_pioglitazone = csvRecord.get(40);
			setBinaryVec(metformin_pioglitazone, values, 121);

			String change = csvRecord.get(41);
			setBinaryVec(change, values, 123);

			String diabetesMed = csvRecord.get(42);
			setBinaryVec(diabetesMed, values, 125);

			String readmitted = csvRecord.get(43);
			setBinaryVec(readmitted, values, 127);

			userVector = new Vector(values);
			vectors.put(rowNumber, userVector);
			admissionPatientMap.put(rowNumber, csvRecord.get(0));

			values = new double[129];
			rowNumber++;
		}

		csvParser.close();
		return vectors;
	}

	/**
	 * Needed for {@code vectorizePatients()} to create a sub-vector out of binary
	 * features.
	 * 
	 * @param csvValue
	 *            binary from 1 and -1
	 * @param inValues
	 *            array holding the final vector
	 * @param atArrayPos
	 *            announces position in huge vector
	 */
	private static void setBinaryVec(String csvValue, double[] inValues, int atArrayPos) {
		if (csvValue.equals("1")) {
			inValues[atArrayPos] = 1;
			inValues[atArrayPos + 1] = 0;
		} else if (csvValue.equals("-1")) {
			inValues[atArrayPos] = 0;
			inValues[atArrayPos + 1] = 1;
		}
	}

	/**
	 * Needed for {@code vectorizePatients()} to create a sub-vector out of
	 * threefold features.
	 * 
	 * @param csvValue
	 *            binary from 1, 0, -1
	 * @param inValues
	 *            array holding the final vector
	 * @param atArrayPos
	 *            announces position in huge vector
	 */
	private static void setTripleVec(String csvValue, double[] inValues, int atArrayPos) {
		if (csvValue.equals("1")) {
			inValues[atArrayPos] = 1;
			inValues[atArrayPos + 1] = 0;
			inValues[atArrayPos + 2] = 0;
		} else if (csvValue.equals("-1")) {
			inValues[atArrayPos] = 0;
			inValues[atArrayPos + 1] = 1;
			inValues[atArrayPos + 2] = 0;
		} else {
			inValues[atArrayPos] = 0;
			inValues[atArrayPos + 1] = 0;
			inValues[atArrayPos + 2] = 1;
		}
	}

	/**
	 * Needed for {@code vectorizePatients()} to create a sub-vector out of finitely
	 * amount of features using the csv-value to set 0 or 1 at appropriate
	 * sub-vector's position.
	 * 
	 * @param csvValue
	 *            read csv-value
	 * @param inValues
	 *            vector array
	 * @param startingInArrayAt
	 *            giving the sub-vector's position in vector
	 * @param withNumberOfItems
	 *            giving the sub-vector's length in vector
	 */
	private static void setDistinctVecFor(String csvValue, double[] inValues, int startingInArrayAt,
			int withNumberOfItems) {
		int id = 0;

		while (id != withNumberOfItems) {
			int id_comp = id + 1;
			if (id_comp == Integer.parseInt(csvValue)) {
				int valID = startingInArrayAt + id;
				inValues[valID] = 1;
			} else {
				int valID = startingInArrayAt + id;
				inValues[valID] = 0;
			}
			id++;
		}
	}

	/**
	 * Calculates patients' cosine, euclidean, and manhattan distance function and
	 * put the result in the appropriate file.
	 * 
	 * @param patientVectors
	 *            created Map of patient vectors from {@code vectorizePatient()}
	 * @param vmID
	 *            handed over by VM class
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static void calcDistance(Map<Integer, Map<Integer, Vector>> patientVectors, int vmID)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {
		CosineDistanceFunction cdf = new CosineDistanceFunction();
		EuclideanDistanceFunction edf = new EuclideanDistanceFunction();
		ManhattanDistanceFunction mdf = new ManhattanDistanceFunction();

		try {
			System.out.println("Step 2 \t CALCULATION OF");

			if (vmID == 1) {
				Writer writerCosine = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/cosine_VM1"), "utf-8"));
				Writer writerEuclidean = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/euclidean_VM1"), "utf-8"));
				Writer writerManhattan = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/manhattan_VM1"), "utf-8"));

				System.out.println("\t Step 2.1 \t COSINE DISTANCES");
				distanceCalcTime(patientVectors, cdf, null, null, writerCosine, null, null,vmID);
				System.out.println("\t Step 2.2 \t EUCLIDEAN DISTANCES");
				distanceCalcTime(patientVectors, null, edf, null, null, writerEuclidean, null,vmID);
				System.out.println("\t Step 2.3 \t MANHATTAN DISTANCES");
				distanceCalcTime(patientVectors, null, null, mdf, null, null, writerManhattan,vmID);
			} else if (vmID == 2) {
				Writer writerCosine = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/cosine_VM2"), "utf-8"));
				Writer writerEuclidean = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/euclidean_VM2"), "utf-8"));
				Writer writerManhattan = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/manhattan_VM2"), "utf-8"));

				System.out.println("\t Step 2.1 \t COSINE DISTANCES");
				distanceCalcTime(patientVectors, cdf, null, null, writerCosine, null, null,vmID);
				System.out.println("\t Step 2.2 \t EUCLIDEAN DISTANCES");
				distanceCalcTime(patientVectors, null, edf, null, null, writerEuclidean, null,vmID);
				System.out.println("\t Step 2.3 \t MANHATTAN DISTANCES");
				distanceCalcTime(patientVectors, null, null, mdf, null, null, writerManhattan,vmID);
			} else {
				Writer writerCosine = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/cosine_VM3"), "utf-8"));
				Writer writerEuclidean = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/euclidean_VM3"), "utf-8"));
				Writer writerManhattan = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream("/home/nicole.sarna/elkiDistancesVM/elkiExample/src/output/manhattan_VM3"), "utf-8"));

				System.out.println("\t Step 2.1 \t COSINE DISTANCES");
				distanceCalcTime(patientVectors, cdf, null, null, writerCosine, null, null,vmID);
				System.out.println("\t Step 2.2 \t EUCLIDEAN DISTANCES");
				distanceCalcTime(patientVectors, null, edf, null, null, writerEuclidean, null,vmID);
				System.out.println("\t Step 2.3 \t MANHATTAN DISTANCES");
				distanceCalcTime(patientVectors, null, null, mdf, null, null, writerManhattan,vmID);
			}
		} finally {
		}
	}

	private static void chunkingCalc(Map<Integer, Vector> vectors, int vmID)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {
		int compNumOfVec = 33365;
		int id = 1;
		int joinID = 1;
		int length;
		int rowNumber;

		Map<Integer, Vector> subVectors = new HashMap<>();
		Map<Integer, Map<Integer, Vector>> finalVectors = new HashMap<>();

		if (vmID == 1) {
			rowNumber = 1;
			length = 33365;
		} else if (vmID == 2) {
			rowNumber = 33366;
			length = 66730;
		} else {
			rowNumber = 66731;
			length = 100096;
			compNumOfVec = 33366;
		}

		for (int i = rowNumber; i <= length; i++) {
			subVectors.put(i, vectors.get(i));

			if (i < vectors.size() / 2) {
				joinID = i + 1;

				while ((joinID <= (i + (vectors.size() / 2))) && (id <= compNumOfVec)) {
					subVectors.put(joinID, vectors.get(joinID));
					joinID = joinID + 1;
					id += 1;
				}
			} else if (i == vectors.size()) {
				while ((joinID < vectors.size() / 2) && (id <= compNumOfVec)) {
					subVectors.put(joinID, vectors.get(joinID));
					joinID = joinID + 1;
					id += 1;
				}
			} else {
				while (((id <= compNumOfVec)
						&& (joinID < (vectors.size() / 2 - (vectors.size() - i) % (vectors.size() / 2))))) {
					subVectors.put(joinID, vectors.get(joinID));
					joinID = joinID + 1;
					id += 1;
				}

				joinID = i + 1;

				while ((id <= compNumOfVec) && ((joinID > i) && (joinID <= vectors.size()))) {
					subVectors.put(joinID, vectors.get(joinID));
					joinID = joinID + 1;
					id += 1;
				}
			}
			finalVectors.put(i, subVectors);
		}
		calcDistance(finalVectors, vmID);
	}

	private static void distanceCalcTime(Map<Integer, Map<Integer, Vector>> patientVectors, CosineDistanceFunction cdf,
			EuclideanDistanceFunction edf, ManhattanDistanceFunction mdf, Writer writerCosine, Writer writerEuclidean,
			Writer writerManhattan, int vmID) throws IOException {

		int start;
		int length;

		if (vmID == 1) {
			start = 1;
			length = 33365;
		} else if (vmID == 2) {
			start = 33366;
			length = 66730;
		} else {
			start = 66731;
			length = 100096;
		}

		long startTime = System.nanoTime();

		if (cdf != null) {
			for (int id = start; id <= length; id++) {
				for (Integer key : patientVectors.get(id).keySet()) {
					writerCosine.write(admissionPatientMap.get(id) + "," + admissionPatientMap.get(key) + ","
							+ cdf.distance(patientVectors.get(id).get(id), patientVectors.get(id).get(key)) + "\n");
				}
			}
		} else if (edf != null) {
			for (int id = start; id <= length; id++) {
				for (Integer key : patientVectors.get(id).keySet()) {
					writerEuclidean.write(admissionPatientMap.get(id) + "," + admissionPatientMap.get(key) + ","
							+ edf.distance(patientVectors.get(id).get(id), patientVectors.get(id).get(key)) + "\n");
				}
			}
		} else {
			for (int id = start; id <= length; id++) {
				for (Integer key : patientVectors.get(id).keySet()) {
					writerManhattan.write(admissionPatientMap.get(id) + "," + admissionPatientMap.get(key) + ","
							+ mdf.distance(patientVectors.get(id).get(id), patientVectors.get(id).get(key)) + "\n");
				}
			}
		}

		long stopTime = System.nanoTime();
		long duration = stopTime - startTime;
		final double minutes = ((double) duration * 0.0000000000166667);
		System.out.println(" -> " + new DecimalFormat("###.##########").format(minutes) + " min");
	}
}
