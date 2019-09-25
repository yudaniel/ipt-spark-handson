package ch.ipt.handson;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.expr;

public class SparkJoins {
    public static void main(String[] args) {
        // Create a new local SparkSession
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .getOrCreate();

        // Set the loglevel to ERROR
        SparkContext sc = SparkContext.getOrCreate();
        sc.setLogLevel("ERROR");

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDataset = spark.createDataset(
                Arrays.asList(
                        new Person(0, "Suzannah Oneal", 43, 0, Arrays.asList(200, 150)),
                        new Person(1, "Nida Partridge", 34, 1, Arrays.asList(100, 150, 200)),
                        new Person(2, "Angelo Osborne", 25, 1, Arrays.asList(100))
                ),
                personEncoder);
        personDataset.show();

        Encoder<Company> companyEncoder = Encoders.bean(Company.class);
        Dataset<Company> companyDataset = spark.createDataset(
                Arrays.asList(
                        new Company(0, "Zurich", "IT", "Sparkz"),
                        new Company(1, "Zurich", "Consulting", "BigData4All"),
                        new Company(2, "New York", "IT", "WorkHardPlayHard")
                ),
                companyEncoder);
        companyDataset.show();

        Encoder<PositionStatus> positionStatusEncoder = Encoders.bean(PositionStatus.class);
        Dataset<PositionStatus> positionStatusDataset = spark.createDataset(
                Arrays.asList(
                        new PositionStatus(100, "Data Scientist"),
                        new PositionStatus(150, "Data Engineer"),
                        new PositionStatus(200, "Software Engineer")
                ),
                positionStatusEncoder);
        positionStatusDataset.show();

        Column sameCompanyJoinExpr = personDataset.col("company").equalTo(companyDataset.col("id"));
        personDataset.join(companyDataset, sameCompanyJoinExpr).show();
        personDataset.join(companyDataset, sameCompanyJoinExpr, "outer").show();
        companyDataset.join(personDataset, sameCompanyJoinExpr, "left_outer").show();
        companyDataset.join(personDataset, sameCompanyJoinExpr, "left_semi").show();
        companyDataset.join(personDataset, sameCompanyJoinExpr, "left_anti").show();

        Dataset<Row> personPosition = personDataset.withColumnRenamed("id", "personId")
                .join(positionStatusDataset, expr("array_contains(positionStatus, status)"));
        personPosition.show();
        personPosition.join(companyDataset, personPosition.col("company").equalTo(companyDataset.col("id"))).show();
    }

    public static class Person implements Serializable {
        private int id;
        private String name;
        private int age;
        private int company;
        private List<Integer> positionStatus;

        public Person(int id, String name, int age, int company, List<Integer> positionStatus) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.company = company;
            this.positionStatus = positionStatus;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getCompany() {
            return company;
        }

        public void setCompany(int company) {
            this.company = company;
        }

        public List<Integer> getPositionStatus() {
            return positionStatus;
        }

        public void setPositionStatus(List<Integer> positionStatus) {
            this.positionStatus = positionStatus;
        }
    }

    public static class Company implements Serializable {
        private int id;
        private String headquarters;
        private String industry;
        private String companyName;

        public Company(int id, String headquarters, String industry, String companyName) {
            this.id = id;
            this.headquarters = headquarters;
            this.industry = industry;
            this.companyName = companyName;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getHeadquarters() {
            return headquarters;
        }

        public void setHeadquarters(String headquarters) {
            this.headquarters = headquarters;
        }

        public String getIndustry() {
            return industry;
        }

        public void setIndustry(String industry) {
            this.industry = industry;
        }

        public String getCompanyName() {
            return companyName;
        }

        public void setCompanyName(String companyName) {
            this.companyName = companyName;
        }
    }

    public static class PositionStatus implements Serializable {
        private int status;
        private String position;

        public PositionStatus(int status, String position) {
            this.status = status;
            this.position = position;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public String getPosition() {
            return position;
        }

        public void setPosition(String position) {
            this.position = position;
        }
    }
}
