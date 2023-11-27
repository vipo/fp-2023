import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import Test.Hspec
import DataFrame
import Data.Either

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
    it "doesn't execute wrong statements" $ do
      Lib2.parseStatement "shuow teibls;" `shouldSatisfy` isLeft
    it "doesn'execute statements without ';'" $ do
      Lib2.parseStatement "show tables" `shouldSatisfy` isLeft
    it "shows the right tables" $ do
      Lib2.parseStatement "show table employees;" `shouldBe` Right (ShowTable "employees")
    it "doesn't show wrong tables" $ do
      Lib2.parseStatement "show table Flags;" `shouldSatisfy` isLeft
    it "doesn't show wrong tables" $ do
      Lib2.parseStatement "show table Flagiuks;" `shouldSatisfy` isLeft
    it "shows the list of tables" $ do
      Lib2.parseStatement "show tables;" `shouldBe` Right ShowTables
    it "shows the list of tables" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right ShowTables
    it "shows the list of tables" $ do
      Lib2.parseStatement "SHOW TABLES     ;         " `shouldSatisfy` isLeft
    it "shows the list of tables" $ do
      Lib2.parseStatement "SHOW TABLES     ;" `shouldBe` Right ShowTables
    it "shows the right column" $ do
      Lib2.parseStatement "select id from employees;" `shouldBe` Right (Select (SelectColumns ["id"]) "employees" Nothing)
    it "shows right multiple columns" $ do
      Lib2.parseStatement "select id, name from employees;" `shouldBe` Right (Select (SelectColumns ["id", "name"]) "employees" Nothing)
    it "shows right multiple columns with many whitespaces" $ do
      Lib2.parseStatement "select      id     ,      name     from      employees;" `shouldBe` Right (Select (SelectColumns ["id", "name"]) "employees" Nothing)
    it "doesn't execute queries without a ';'" $ do
      Lib2.parseStatement "select id, name from employees" `shouldSatisfy` isLeft
    it "shows the max of the specified column from the specified table" $ do
      Lib2.parseStatement "select max(id) from employees;" `shouldBe` Right (Select (SelectAggregate [(Max, "id")]) "employees" Nothing);
    it "shows multiple aggregate functions" $ do
      Lib2.parseStatement "select max(id), sum(id) from employees;" `shouldBe` Right (Select (SelectAggregate [(Max, "id"), (Sum, "id")]) "employees" Nothing)
    it "shows multiple aggregate functions with many whitespaces" $ do
      Lib2.parseStatement "select max    (       id)    ,    sum     (    id    ) from employees;" `shouldBe` Right (Select (SelectAggregate [(Max, "id"), (Sum, "id")]) "employees" Nothing)
    it "doesn't let to mismatch aggregates and columns" $ do
      Lib2.parseStatement "select max(id), id from employees;" `shouldSatisfy` isLeft
    it "doesn't let to put multiple columns in the aggregate function" $ do
      Lib2.parseStatement "select max(id, name) from employees;" `shouldSatisfy` isLeft
    it "executes the right query with simple where conditions" $ do
      Lib2.parseStatement "select id from employees where id = 1;" `shouldBe` Right (Select {table = "employees", selectQuery = (SelectColumns ["id"]), selectWhere = Just [(Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1)))]})
    it "executes the right query with where conditions that are always true" $ do
      Lib2.parseStatement "select id, name from employees where 1 = 1;" `shouldBe` Right (Select {table = "employees", selectQuery = (SelectColumns ["id","name"]), selectWhere = Just [(Condition (ConstantOperand(IntegerValue 1)) IsEqualTo (ConstantOperand (IntegerValue 1)))]})
    it "executes the right query with where conditions that are not valid with provided operator" $ do
      Lib2.parseStatement "select id from employees where name > surname;" `shouldBe` Right (Select {table = "employees", selectQuery = (SelectColumns ["id"]), selectWhere = Just [(Condition (ColumnOperand "name") IsGreaterThan (ColumnOperand "surname"))]})
    it "executes the right query with where conditions that have 'and'" $ do
      Lib2.parseStatement "select id from employees where name = 'Vi' and id = 1;" `shouldBe` Right (Select {table = "employees", selectQuery = (SelectColumns ["id"]), selectWhere = Just [(Condition (ColumnOperand "name") IsEqualTo (ConstantOperand (StringValue "Vi"))),(Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1)))]})
    it "executes the right query with where conditions that have several 'and'" $ do
      Lib2.parseStatement "select id from employees where name = 'Vi' and id = 1 and surname = 'Po';" `shouldBe` Right (Select {table = "employees", selectQuery = (SelectColumns ["id"]), selectWhere = Just [(Condition (ColumnOperand "name") IsEqualTo (ConstantOperand (StringValue "Vi"))),(Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1))),(Condition (ColumnOperand "surname") IsEqualTo (ConstantOperand (StringValue "Po")))]})
    it "doesn't let to execute with columns not listed right" $ do
      Lib2.parseStatement "select id, from employees;" `shouldSatisfy` isLeft
    it "doesn't let to execute with columns not listed right" $ do
      Lib2.parseStatement "select id, from employees;" `shouldSatisfy` isLeft
  describe "Lib2.executeStatement" $ do
    it "it should output the right table with 'select * from 'tablename';'" $ do
      Lib2.executeStatement SelectAll {table = "employees", selectWhere = Nothing} `shouldBe` Right (snd D.tableEmployees)
    it "it should not output the invalid table with 'select * from 'tablename';'" $ do
      Lib2.executeStatement SelectAll {table = "invalid2", selectWhere = Nothing} `shouldSatisfy` isLeft
    it "executes 'show table 'tablename';'" $ do
      Lib2.executeStatement ShowTable {table = "employees"} `shouldSatisfy` isRight
    it "executes 'show tables;'" $ do
      Lib2.executeStatement ShowTables {} `shouldSatisfy` isRight  
    it "doesn't execute a select statement with non existant 'tablename' " $ do
      Lib2.executeStatement SelectAll {table = "ananasas", selectWhere = Just [(Condition (ColumnOperand "abrikosas") IsGreaterOrEqual (ColumnOperand "surname"))]} `shouldSatisfy` isLeft
    it "does not find provided non-existing columns in the table" $ do
      Lib2.executeStatement Select {selectQuery = (SelectColumns ["abrikosas", "bananas"]), table = "employees", selectWhere = Nothing } `shouldSatisfy` isLeft 
    it "doesn't let to sum non-integer values" $ do
      Lib2.executeStatement Select {selectQuery = (SelectAggregate [(Sum, "name")]), table = "employees", selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to find max of non-existing column" $ do
      Lib2.executeStatement Select {selectQuery = (SelectAggregate [(Max, "flag")]), table = "employees", selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to compare when condition is faulty" $ do
      Lib2.executeStatement Select {selectQuery = (SelectColumns [("id")]), table = "employees", selectWhere = Just [(Condition (ConstantOperand(StringValue "labas")) IsGreaterOrEqual (ConstantOperand (BoolValue True)))] } `shouldSatisfy` isLeft
      