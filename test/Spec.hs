import Data.Either
import Data.Maybe ()
import DataFrame
import InMemoryTables qualified as D
import Lib1
import Lib2 (ParsedStatement (..), Operator(..), parseStatement, executeStatement)
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
  describe "Lib2.parseStatement" $ do
    it "Parses SHOW TABLES statement" $ do
      Lib2.parseStatement "SHOW TABLES" `shouldBe` Right ShowTables
    it "Parses SHOW TABLE statement" $ do
      Lib2.parseStatement "show table flags" `shouldBe` Right (ShowTable "flags")
    it "Parses SELECT id FROM statement" $ do
      Lib2.parseStatement "SELECT id FROM employees" `shouldBe` Right (Select ["id"] "employees" Nothing)
    it "Parses SELECT statement with case-sensitive columns and table names, ignoring SQL keyword case" $ do
      Lib2.parseStatement "SelecT Id FroM Employees" `shouldBe` Right (Select ["Id"] "Employees" Nothing)
    it "Handles empty input" $ do
      Lib2.parseStatement "" `shouldSatisfy` isLeft
    it "Handles not supported statements" $ do
      Lib2.parseStatement "wrong statement" `shouldSatisfy` isLeft
    it "Handles empty select input" $ do
      Lib2.parseStatement "select" `shouldSatisfy` isLeft
    it "Works with WHERE statements" $ do
      Lib2.parseStatement "select * from employees where id = 1" `shouldBe` Right (Select ["*"] "employees" (Just [Operator "id" "=" (IntegerValue 1)]))
    it "WHERE statements work with strings" $ do
      Lib2.parseStatement "select * from employees where name = \"Vi\"" `shouldBe` Right (Select ["*"] "employees" (Just [Operator "name" "=" (StringValue "Vi")]))
    it "Works with WHERE statements with multiple ANDs" $ do
      Lib2.parseStatement "select * from employees where id > 1 and name = \"Vi\" and surname = \"Po\"" `shouldBe` Right (Select ["*"] "employees" (Just [Operator "id" ">" (IntegerValue 1), Operator "name" "=" (StringValue "Vi"), Operator "surname" "=" (StringValue "Po")]))
    it "Handles where statement with incompatible operator" $ do
      Lib2.parseStatement "select * from employees where id is 1" `shouldSatisfy` isLeft
    it "Handles incorrect where syntax" $ do
      Lib2.parseStatement "select * from employees where" `shouldSatisfy` isLeft
  describe "Lib2.executeStatement" $ do
    it "Returns the SHOW TABLES dataframe correctly" $ do
      Lib2.executeStatement ShowTables `shouldBe` Right (DataFrame [Column "tables" StringType] [[StringValue "employees"], [StringValue "invalid1"], [StringValue "invalid2"], [StringValue "long_strings"], [StringValue "flags"]])
    it "Returns a SHOW TABLE dataframe correctly" $ do
      Lib2.executeStatement (ShowTable "employees") `shouldBe` Right (DataFrame [Column "columns" StringType] [[StringValue "id"], [StringValue "name"], [StringValue "surname"]])
    it "Handles not existing tables with SHOW TABLE" $ do
      Lib2.executeStatement (ShowTable "nothing") `shouldSatisfy` isLeft
    it "Returns a Dataframe with SELECT * correctly" $ do
      Lib2.executeStatement (Select ["*"] "employees" Nothing) `shouldBe` Right (snd D.tableEmployees)
    it "Returns a DataFrame with a SELECT with a specific column correctly" $ do
      Lib2.executeStatement (Select ["id"] "employees" Nothing) `shouldBe` Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2], [IntegerValue 3], [IntegerValue 4]])
    it "Handles a SELECT statement with a nonexistent table" $ do
      Lib2.executeStatement (Select ["*"] "nothing" Nothing) `shouldSatisfy` isLeft
    it "Handles a SELECT statement with a nonexistent column" $ do
      Lib2.executeStatement (Select ["nothing"] "employees" Nothing) `shouldSatisfy` isLeft
    it "Returns a DataFrame with SELECT with where statement" $ do
      Lib2.executeStatement (Select ["*"] "employees" (Just [Operator "id" "=" (IntegerValue 1)])) `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
    it "Returns a DataFrame with SELECT with where case with string value" $ do
      Lib2.executeStatement (Select ["*"] "employees" (Just [Operator "name" "=" (StringValue "Vi")])) `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
    it "Handles wrong select min syntax" $ do
      Lib2.executeStatement (Select ["min( id)"] "employees" Nothing) `shouldSatisfy` isLeft
    it "Returns a DataFrame with SELECT min correctly" $ do
      Lib2.executeStatement (Select ["min(id)"] "employees" Nothing) `shouldBe` Right (DataFrame [Column "minimum" IntegerType] [[IntegerValue 1]])
    it "Returns a DataFrame with SELECT sum with where case correctly" $ do
      Lib2.executeStatement (Select ["sum(id)"] "employees" (Just [Operator "id" ">" (IntegerValue 2)])) `shouldBe` Right (DataFrame [Column "sum" IntegerType] [[IntegerValue 7]])
    it "Returns a DataFrame with SELECT with WHERE AND" $ do
      Lib2.executeStatement (Select ["*"] "employees" (Just [Operator "id" ">" (IntegerValue 1), Operator "name" "=" (StringValue "Ed")])) `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    it "Returns a DataFrame with SELECT with WHERE with multiple AND statements" $ do
      Lib2.executeStatement (Select ["*"] "employees" (Just [Operator "id" ">" (IntegerValue 1), Operator "surname" "=" (StringValue "Dl"), Operator "name" "=" (StringValue "Ed")])) `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 2, StringValue "Ed", StringValue "Dl"]])