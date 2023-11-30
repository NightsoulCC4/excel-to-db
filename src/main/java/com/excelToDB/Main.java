package com.excelToDB;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;

import java.sql.*;
import java.text.DecimalFormat;

public class Main {
    // Declear constant variable here.
    public static Properties properties = null;

    public static void main(String[] args) {

        properties = new Properties();

        // Load properties file.
        try (FileInputStream configFile = new FileInputStream("config.properties")) {
            properties.load(configFile);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Load path.
        Path directoryToWatch = Paths.get(properties.getProperty("initial_path"));

        System.out.println("Begining initalized path.");
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            directoryToWatch.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            while (true) {
                WatchKey key;

                try {
                    Thread.sleep(5000);

                    System.out.println("Initializing path.");

                    key = watchService.take();
                    if (key == null)
                        continue;

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW)
                        continue;

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path fileName = pathEvent.context();
                    File inputFile = new File(directoryToWatch.resolve(fileName).toString());
                    // File outputFile = new File(export_path);

                    FileInputStream excelFile = new FileInputStream(inputFile);
                    Workbook workbook = null;

                    ObjectMapper mapper = new ObjectMapper();
                    List<String> jsonArray = new ArrayList<>();

                    // Add case file extendion here, if the customer wants to add requirement.
                    if (fileName.toString().endsWith(".xlsx"))
                        workbook = new XSSFWorkbook(excelFile);
                    else if (fileName.toString().endsWith(".xls"))
                        workbook = new HSSFWorkbook(excelFile);
                    else
                        // code here if you want to do somethings with a mismatch file type.
                        System.out.println("Mismatch file type.");

                    if (workbook != null) {
                        Sheet sheet = workbook.getSheetAt(0);
                        Iterator<Row> rowIterator = sheet.iterator();

                        jsonArray = getDataFromExcel(jsonArray, rowIterator);

                        // outputFile = ExportJson(inputFile, outputFile, mapper, jsonArray);

                        StoreInDB(jsonArray);

                        workbook.close();

                        MoveFile(inputFile, jsonArray, mapper);
                    }
                }
                System.out.println("End process.");
                key.reset();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static List<String> getDataFromExcel(List<String> jsonArray,
            Iterator<Row> rowIterator) throws IOException {

        DecimalFormat decimalFormat = new DecimalFormat("#");

        try {
            // Skip the header row.
            rowIterator.next();

            // Iterate through the rows and create a JSON object for each row.
            while (rowIterator.hasNext()) {
                Row currentRow = rowIterator.next();
                Iterator<Cell> cellIterator = currentRow.iterator();

                while (cellIterator.hasNext()) {
                    Cell currentCell = cellIterator.next();
                    // String header = headers.get(headerIndex);

                    // Use column index as the JSON key name if header is a number and not a number.
                    if (currentCell.getCellType() == CellType.NUMERIC) {
                        // System.out.println(currentCell.getDateCellValue());
                        try {
                            jsonArray.add(String.format("1" + decimalFormat.format(currentCell.getNumericCellValue())));
                            System.out.println(jsonArray);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return jsonArray;
        }

        return jsonArray;
    }

    public static void StoreInDB(List<String> jsonArray) {
        try {
            // Load the PostgreSQL JDBC driver.
            Class.forName("org.postgresql.Driver");

            Connection c = DriverManager.getConnection((String) properties.getProperty("jdbcurl"),
                    (String) properties.getProperty("username"), (String) properties.getProperty("password"));

            // SQL statement to insert JSON data
            String sql = "UPDATE order_item SET " +
                    "fix_order_status_id = '" + 6 + "' " +
                    ", remain_report_date = current_date::text " +
                    ", remain_report_note = '"
                    + new String(properties.getProperty("note").getBytes(StandardCharsets.ISO_8859_1),
                            StandardCharsets.UTF_8)
                    + "' " +
                    ", remain_report_eid = '" + (String) properties.getProperty("eid") + "' " +
                    "WHERE assigned_ref_no = ? " +
                    "AND fix_order_status_id = '2' " +
                    "AND item_id IN " + formatItemId((String) properties.getProperty("item_id"));
            // Create a prepared statement.
            PreparedStatement ps = c.prepareStatement(sql);

            if (jsonArray.size() > 1)
                for (String value : jsonArray) {
                    ps.setString(1, value);
                    ps.executeUpdate();
                }
            else {
                ps.setString(1, jsonArray.get(0));
                ps.executeUpdate();
            }

            // Close the prepared statement and connection.
            ps.close();
            ps = null;
            c.close();
            c = null;
            System.out.println("Update successful!");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } 
    }

    public static File ExportJson(File inputFile, File outputDirectory, ObjectMapper mapper, List<String> jsonArray)
            throws IOException {

        String exportFileName = null;

        // Determine the JSON file name.
        if (inputFile.getName().endsWith(".xlsx"))
            exportFileName = inputFile.getName().replace(".xlsx", ".json");
        else if (inputFile.getName().endsWith(".xls"))
            exportFileName = inputFile.getName().replace(".xls", ".json");

        // Specify the target directory for the JSON file.
        File outputFile = new File(outputDirectory, exportFileName);
        mapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, jsonArray);

        System.out.println("Excel file to JSON conversion completed.");

        return outputFile;
    }

    public static void MoveFile(File inputFile, /* File outputFile, */
            List<String> jsonArray, ObjectMapper mapper)
            throws IOException {

        // Move the JSON file to a export location.
        /*
         * Path sourcePath = outputFile.toPath();
         * Path exportPath = Paths.get(export_path).resolve(outputFile.getName());
         * Files.move(sourcePath, exportPath, StandardCopyOption.REPLACE_EXISTING);
         */

        // Move the Xlsx file to a backup location.
        Path sourceBackupPath = inputFile.toPath();
        Path backupPath = Paths.get(properties.getProperty("backup_path")).resolve(inputFile.getName());
        Files.move(sourceBackupPath, backupPath, StandardCopyOption.REPLACE_EXISTING);

        System.out.println("Move file success. " + LocalDateTime.now());

    }

    private static String formatItemId(String item_id) {
        String[] parts = item_id.split(",");
        for (int i = 0; i < parts.length; i++)
            parts[i] = "'" + parts[i].trim() + "'";

        // Join the parts into a new string with single quotes
        String result = "(" + String.join(", ", parts) + ")";

        return result;
    }

}