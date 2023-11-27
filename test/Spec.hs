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
      Lib2.parseStatement "show table employees;" `shouldBe` Right (Lib2.ShowTable "employees")
    it "doesn't show wrong tables" $ do
      Lib2.parseStatement "show table Flags;" `shouldSatisfy` isLeft
    it "doesn't show wrong tables" $ do
      Lib2.parseStatement "show table Flagiuks;" `shouldSatisfy` isLeft
    it "shows the list of tables" $ do
      Lib2.parseStatement "show tables;" `shouldBe` Right Lib2.ShowTables
    it "shows the list of tables" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right Lib2.ShowTables
    it "shows the list of tables" $ do
      Lib2.parseStatement "SHOW TABLES     ;         " `shouldSatisfy` isLeft
    it "shows the list of tables" $ do
      Lib2.parseStatement "SHOW TABLES     ;" `shouldBe` Right Lib2.ShowTables
    it "shows the right column" $ do
      Lib2.parseStatement "select id from employees;" `shouldBe` Right (Lib2.Select (SelectColumns ["id"]) "employees" Nothing)
    it "shows right multiple columns" $ do
      Lib2.parseStatement "select id, name from employees;" `shouldBe` Right (Lib2.Select (SelectColumns ["id", "name"]) "employees" Nothing)
    it "shows right multiple columns with many whitespaces" $ do
      Lib2.parseStatement "select      id     ,      name     from      employees;" `shouldBe` Right (Lib2.Select (SelectColumns ["id", "name"]) "employees" Nothing)
    it "doesn't execute queries without a ';'" $ do
      Lib2.parseStatement "select id, name from employees" `shouldSatisfy` isLeft
    it "shows the max of the specified column from the specified table" $ do
      Lib2.parseStatement "select max(id) from employees;" `shouldBe` Right (Lib2.Select (SelectAggregate [(Max, "id")]) "employees" Nothing);
    it "shows multiple aggregate functions" $ do
      Lib2.parseStatement "select max(id), sum(id) from employees;" `shouldBe` Right (Lib2.Select (SelectAggregate [(Max, "id"), (Sum, "id")]) "employees" Nothing)
    it "shows multiple aggregate functions with many whitespaces" $ do
      Lib2.parseStatement "select max    (       id)    ,    sum     (    id    ) from employees;" `shouldBe` Right (Lib2.Select (SelectAggregate [(Max, "id"), (Sum, "id")]) "employees" Nothing)
    it "doesn't let to mismatch aggregates and columns" $ do
      Lib2.parseStatement "select max(id), id from employees;" `shouldSatisfy` isLeft
    it "doesn't let to put multiple columns in the aggregate function" $ do
      Lib2.parseStatement "select max(id, name) from employees;" `shouldSatisfy` isLeft
    it "executes the right query with simple where conditions" $ do
      Lib2.parseStatement "select id from employees where id = 1;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1))]})
    it "executes the right query with where conditions that are always true" $ do
      Lib2.parseStatement "select id, name from employees where 1 = 1;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = SelectColumns ["id","name"], Lib2.selectWhere = Just [Condition (ConstantOperand (IntegerValue 1)) IsEqualTo (ConstantOperand (IntegerValue 1))]})
    it "executes the right query with where conditions that are not valid with provided operator" $ do
      Lib2.parseStatement "select id from employees where name > surname;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "name") IsGreaterThan (ColumnOperand "surname")]})
    it "executes the right query with where conditions that have 'and'" $ do
      Lib2.parseStatement "select id from employees where name = 'Vi' and id = 1;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "name") IsEqualTo (ConstantOperand (StringValue "Vi")),Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1))]})
    it "executes the right query with where conditions that have several 'and'" $ do
      Lib2.parseStatement "select id from employees where name = 'Vi' and id = 1 and surname = 'Po';" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "name") IsEqualTo (ConstantOperand (StringValue "Vi")),Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1)),Condition (ColumnOperand "surname") IsEqualTo (ConstantOperand (StringValue "Po"))]})
    it "doesn't let to execute with columns not listed right" $ do
      Lib2.parseStatement "select id, from employees;" `shouldSatisfy` isLeft
    it "doesn't let to execute with columns not listed right" $ do
      Lib2.parseStatement "select id, from employees;" `shouldSatisfy` isLeft
  describe "Lib2.executeStatement" $ do
    it "it should output the right table with 'select * from 'tablename';'" $ do
      Lib2.executeStatement Lib2.SelectAll {Lib2.table = "employees", Lib2.selectWhere = Nothing} `shouldBe` Right (snd D.tableEmployees)
    it "it should not output the invalid table with 'select * from 'tablename';'" $ do
      Lib2.executeStatement Lib2.SelectAll {Lib2.table = "invalid2", Lib2.selectWhere = Nothing} `shouldSatisfy` isLeft
    it "executes 'show table 'tablename';'" $ do
      Lib2.executeStatement Lib2.ShowTable {Lib2.table = "employees"} `shouldSatisfy` isRight
    it "executes 'show tables;'" $ do
      Lib2.executeStatement Lib2.ShowTables {} `shouldSatisfy` isRight
    it "doesn't execute a select statement with non existant 'tablename' " $ do
      Lib2.executeStatement Lib2.SelectAll {Lib2.table = "ananasas", Lib2.selectWhere = Just [Condition (ColumnOperand "abrikosas") IsGreaterOrEqual (ColumnOperand "surname")]} `shouldSatisfy` isLeft
    it "does not find provided non-existing columns in the table" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = SelectColumns ["abrikosas", "bananas"], Lib2.table = "employees", Lib2.selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to sum non-integer values" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = SelectAggregate [(Sum, "name")], Lib2.table = "employees", Lib2.selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to find max of non-existing column" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = SelectAggregate [(Max, "flag")], Lib2.table = "employees", Lib2.selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to compare when condition is faulty" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = SelectColumns ["id"], Lib2.table = "employees", Lib2.selectWhere = Just [Condition (ConstantOperand (StringValue "labas")) IsGreaterOrEqual (ConstantOperand (BoolValue True))] } `shouldSatisfy` isLeft
  describe "Lib3.parseStatement" $ do
    it "Parses show table statement " $ do
      Lib3.parseStatement2 "show table employees;" `shouldBe` Right (Lib3.ShowTable "employees") 
    it "Parses show tables statement " $ do
      Lib3.parseStatement2 "show tables;" `shouldBe` Right Lib3.ShowTables
    it "Fails to parse show table statement without table name" $ do
      Lib3.parseStatement2 "show table ;" `shouldSatisfy` isLeft
    it "Parses correct delete statement without conditions" $ do
      Lib3.parseStatement2 "delete from flags;" `shouldBe` Right (Lib3.Delete {Lib3.table = "flags",Lib3.conditions =  Nothing})
    it "Parses correct delete statement with conditions" $ do
      Lib3.parseStatement2 "delete from flags where flag = 'b';" `shouldBe` Right Lib3.Delete {Lib3.table = "flags", Lib3.conditions = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses correct insert statement without specified columns " $ do
      Lib3.parseStatement2 "insert into flags values (3, 'c', true);" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Nothing, Lib3.values = [IntegerValue 3, StringValue "c", BoolValue True]}
    it "Parses correct insert statement with one specified columns" $ do
      Lib3.parseStatement2 "insert into flags (id) values (3);" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Just  (ColumnsSelected ["id"]), Lib3.values = [IntegerValue 3]}
    it "Parses correct insert statement with many specified columns" $ do
      Lib3.parseStatement2 "insert into flags (id, flag, value) values (3, 'c', true);" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Just  (ColumnsSelected ["id", "flag","value"]), Lib3.values = [IntegerValue 3, StringValue "c", BoolValue True]}
    it "Parses correct select now() statement " $ do
     Lib3.parseStatement2 "select now();" `shouldBe` Right (Lib3.SelectNow )
    it "Parses correct SELECT NOW() statement with UPPERCASE " $ do
     Lib3.parseStatement2 "SELECT NOW();" `shouldBe` Right (Lib3.SelectNow )
    it "Parses update statement without conditions" $ do
      Lib3.parseStatement2 "update flags set flag = 'a';" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "a"))], Lib3.selectWhere = Nothing}
    it "Fails to parse update statement without table name" $ do
      Lib3.parseStatement2 "update  set flag = 'a';" `shouldSatisfy` isLeft
    it "Parses update statement with conditions" $ do
      Lib3.parseStatement2 "update flags set flag = 'a' where flag = 'b';" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "a"))], Lib3.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses update statement with multiple conditions" $ do
      Lib3.parseStatement2 "update flags set flag = 'a' where flag = 'b' and value = 2;" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "a"))], Lib3.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b")),Condition (ColumnOperand "value") IsEqualTo  (ConstantOperand (IntegerValue 2))]}
    