import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import DataFrame (DataFrame(..), Column(..), Value(..), ColumnType(..))
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

  describe "Lib2" $ do
  
    describe "parseStatement" $ do
      
      it "parses SHOW TABLES case-insensitively" $ do
        parseStatement "sHoW tAbLes;" `shouldBe` Right ShowAllTablesStmt
        parseStatement "SHOW TABLES;" `shouldBe` Right ShowAllTablesStmt
        parseStatement "show tables;" `shouldBe` Right ShowAllTablesStmt
      
      it "parses SHOW TABLE <name> case-insensitively for keywords but case-sensitively for table names" $ do
        parseStatement "SHOW TABLE Employees;" `shouldBe` Right (ShowTableStmt "Employees")
        parseStatement "sHoW tAbLe Employees;" `shouldBe` Right (ShowTableStmt "Employees")
        parseStatement "show table Employees;" `shouldBe` Right (ShowTableStmt "Employees")
        parseStatement "SHOW TABLE employees;" `shouldNotBe` Right (ShowTableStmt "Employees")
      
      it "returns error for statements without semicolon" $ do
        parseStatement "SHOW TABLE Employees" `shouldBe` Left "Unsupported or invalid statement"
        parseStatement "sHoW tAbLes" `shouldBe` Left "Unsupported or invalid statement"
  
      it "returns error for unsupported or invalid statements" $ do
        parseStatement "INVALID STATEMENT;" `shouldBe` Left "Unsupported or invalid statement"
        parseStatement "SHOW COLUMNS FROM Employees;" `shouldBe` Left "Unsupported or invalid statement"
  
    describe "executeStatement" $ do
      it "should list columns for 'SHOW TABLE employees;'" $ do
        let parsed = ShowTableStmt "employees"
        let expectedColumns = [Column "Columns" StringType]
        let expectedRows = [[StringValue "id"], [StringValue "name"], [StringValue "surname"]]
        Lib2.executeStatement parsed `shouldBe` Right (DataFrame expectedColumns expectedRows)

      it "should give an error for a non-existent table" $ do
        let parsed = ShowTableStmt "nonexistent"
        Lib2.executeStatement parsed `shouldBe` Left "Table nonexistent not found"

      it "should list all tables for 'SHOW TABLES;'" $ do
        let parsed = ShowAllTablesStmt
        let expectedColumns = [Column "Tables" StringType]
        let expectedRows = map (\(name, _) -> [StringValue name]) D.database
        Lib2.executeStatement parsed `shouldBe` Right (DataFrame expectedColumns expectedRows)
