import Data.Either
import Data.Maybe ()
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null

  describe "parseStatement" $ do
    it "should parse 'SHOW TABLE employees;' correctly" $ do
      parseStatement "SHOW TABLE employees;" `shouldBe` Right (ShowTable "employees")

    it "should return an error for unsupported statements" $ do
      parseStatement "SELECT * FROM employees;" `shouldBe` Left "Unsupported or invalid statement"

    it "should return an error for malformed statements" $ do
      parseStatement "SHOW employees;" `shouldBe` Left "Unsupported or invalid statement"

    it "should return an error for statements without semicolon" $ do
      parseStatement "SHOW TABLE employees" `shouldBe` Left "Unsupported or invalid statement"

  describe "executeStatement" $ do
    it "should list columns for 'SHOW TABLE employees;'" $ do
      let parsed = ShowTable "employees"
      let expectedColumns = [Column "Columns" StringType]
      let expectedRows = [[StringValue "id"], [StringValue "name"], [StringValue "surname"]]
      executeStatement parsed `shouldBe` Right (DataFrame expectedColumns expectedRows)

    it "should give an error for a non-existent table" $ do
      let parsed = ShowTable "nonexistent"
      executeStatement parsed `shouldBe` Left "Table nonexistent not found"
  describe "parseStatement for SHOW TABLES" $ do
    it "should parse 'SHOW TABLES;' correctly" $ do
      parseStatement "SHOW TABLES;" `shouldBe` Right ShowTables

    it "should return an error for statements without semicolon for SHOW TABLES" $ do
      parseStatement "SHOW TABLES" `shouldBe` Left "Unsupported or invalid statement"

  describe "executeStatement for SHOW TABLES" $ do
    it "should list all tables for 'SHOW TABLES;'" $ do
      let expectedColumns = [Column "Tables" StringType]
      let expectedRows = map (\(name, _) -> [StringValue name]) D.database
      executeStatement ShowTables `shouldBe` Right (DataFrame expectedColumns expectedRows)

  describe "filterRowsByBoolColumn" $ do
    it "should return list of matching rows" $ do
      filterRowsByBoolColumn (snd D.tableWithNulls) (Column "value" BoolType) True `shouldBe` Right DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "a", BoolValue True], [StringValue "b", BoolValue True]]
      filterRowsByBoolColumn (snd D.tableWithNulls) (Column "value" BoolType) False `shouldBe` Right DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "b", BoolValue False]]

    it "should return Error if Column is not bool type" $ do
      filterRowsByBoolColumn (snd D.tableWithNulls) (Column "flag" StringType) True `shouldBe` Left "Dataframe does not contain column by specified name or column is not of type bool"

    it "should return Error if Column is not in table" $ do
      filterRowsByBoolColumn (snd D.tableWithNulls) (Column "flagz" BoolType) True `shouldBe` Left "Dataframe does not contain column by specified name or column is not of type bool"