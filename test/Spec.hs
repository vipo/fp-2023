{-# LANGUAGE ScopedTypeVariables #-}
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import Data.Time ( UTCTime, getCurrentTime )
import Test.Hspec
import DataFrame
import Data.Either
import Data.Time
import Control.Monad.Free (Free (..))
import Data.IORef
import qualified Lib3
import Lib4

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
      Lib2.parseStatement "select id from employees;" `shouldBe` Right (Lib2.Select (Lib2.SelectColumns ["id"]) "employees" Nothing)
    it "shows right multiple columns" $ do
      Lib2.parseStatement "select id, name from employees;" `shouldBe` Right (Lib2.Select (Lib2.SelectColumns ["id", "name"]) "employees" Nothing)
    it "shows right multiple columns with many whitespaces" $ do
      Lib2.parseStatement "select      id     ,      name     from      employees;" `shouldBe` Right (Lib2.Select (Lib2.SelectColumns ["id", "name"]) "employees" Nothing)
    it "doesn't execute queries without a ';'" $ do
      Lib2.parseStatement "select id, name from employees" `shouldSatisfy` isLeft
    it "shows the max of the specified column from the specified table" $ do
      Lib2.parseStatement "select max(id) from employees;" `shouldBe` Right (Lib2.Select (Lib2.SelectAggregate [(Max, "id")]) "employees" Nothing);
    it "shows multiple aggregate functions" $ do
      Lib2.parseStatement "select max(id), sum(id) from employees;" `shouldBe` Right (Lib2.Select (Lib2.SelectAggregate [(Max, "id"), (Sum, "id")]) "employees" Nothing)
    it "shows multiple aggregate functions with many whitespaces" $ do
      Lib2.parseStatement "select max    (       id)    ,    sum     (    id    ) from employees;" `shouldBe` Right (Lib2.Select (Lib2.SelectAggregate [(Max, "id"), (Sum, "id")]) "employees" Nothing)
    it "doesn't let to mismatch aggregates and columns" $ do
      Lib2.parseStatement "select max(id), id from employees;" `shouldSatisfy` isLeft
    it "doesn't let to put multiple columns in the aggregate function" $ do
      Lib2.parseStatement "select max(id, name) from employees;" `shouldSatisfy` isLeft
    it "executes the right query with simple where conditions" $ do
      Lib2.parseStatement "select id from employees where id = 1;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = Lib2.SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1))]})
    it "executes the right query with where conditions that are always true" $ do
      Lib2.parseStatement "select id, name from employees where 1 = 1;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = Lib2.SelectColumns ["id","name"], Lib2.selectWhere = Just [Condition (ConstantOperand (IntegerValue 1)) IsEqualTo (ConstantOperand (IntegerValue 1))]})
    it "executes the right query with where conditions that are not valid with provided operator" $ do
      Lib2.parseStatement "select id from employees where name > surname;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = Lib2.SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "name") IsGreaterThan (ColumnOperand "surname")]})
    it "executes the right query with where conditions that have 'and'" $ do
      Lib2.parseStatement "select id from employees where name = 'Vi' and id = 1;" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = Lib2.SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "name") IsEqualTo (ConstantOperand (StringValue "Vi")),Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1))]})
    it "executes the right query with where conditions that have several 'and'" $ do
      Lib2.parseStatement "select id from employees where name = 'Vi' and id = 1 and surname = 'Po';" `shouldBe` Right (Lib2.Select {Lib2.table = "employees", Lib2.selectQuery = Lib2.SelectColumns ["id"], Lib2.selectWhere = Just [Condition (ColumnOperand "name") IsEqualTo (ConstantOperand (StringValue "Vi")),Condition (ColumnOperand "id") IsEqualTo (ConstantOperand (IntegerValue 1)),Condition (ColumnOperand "surname") IsEqualTo (ConstantOperand (StringValue "Po"))]})
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
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = Lib2.SelectColumns ["abrikosas", "bananas"], Lib2.table = "employees", Lib2.selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to sum non-integer values" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = Lib2.SelectAggregate [(Sum, "name")], Lib2.table = "employees", Lib2.selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to find max of non-existing column" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = Lib2.SelectAggregate [(Max, "flag")], Lib2.table = "employees", Lib2.selectWhere = Nothing } `shouldSatisfy` isLeft
    it "doesn't let to compare when condition is faulty" $ do
      Lib2.executeStatement Lib2.Select {Lib2.selectQuery = Lib2.SelectColumns ["id"], Lib2.table = "employees", Lib2.selectWhere = Just [Condition (ConstantOperand (StringValue "labas")) IsGreaterOrEqual (ConstantOperand (BoolValue True))] } `shouldSatisfy` isLeft
  describe "Lib3.parseStatement" $ do
    it "Parses show table statement " $ do
      Lib3.parseStatement2 "show table employees;" `shouldBe` Right (Lib3.ShowTable "employees") 
    it "Parses show tables statement " $ do
      Lib3.parseStatement2 "show tables;" `shouldBe` Right Lib3.ShowTables
    it "Fails to parse show table statement without table name" $ do
      Lib3.parseStatement2 "show table ;" `shouldSatisfy` isLeft
    it "Parses correct delete statement without conditions" $ do
      Lib3.parseStatement2 "delete from flags;" `shouldBe` Right (Lib3.Delete {Lib3.table = "flags",Lib3.conditions =  Nothing})
    it "Parses correct delete statement with condition" $ do
      Lib3.parseStatement2 "delete from flags where flag = 'b';" `shouldBe` Right Lib3.Delete {Lib3.table = "flags", Lib3.conditions = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses correct delete statement with multiple conditions" $ do
      Lib3.parseStatement2 "delete from flags where flag = 'b' and id = 1;" `shouldBe` Right Lib3.Delete {Lib3.table = "flags", Lib3.conditions = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b")) , Condition (ColumnOperand "id") IsEqualTo  (ConstantOperand (IntegerValue 1))]}
    it "Parses correct insert statement without specified columns " $ do
      Lib3.parseStatement2 "insert into flags values (3, 'c', true);" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Nothing, Lib3.values = [IntegerValue 3, StringValue "c", BoolValue True]}
    it "Parses correct insert statement with one specified columns" $ do
      Lib3.parseStatement2 "insert into flags (id) values (3);" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Just  (Lib3.ColumnsSelected ["id"]), Lib3.values = [IntegerValue 3]}
    it "Parses correct insert statement with many specified columns" $ do
      Lib3.parseStatement2 "insert into flags (id, flag, value) values (3, 'c', true);" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Just  (Lib3.ColumnsSelected ["id", "flag","value"]), Lib3.values = [IntegerValue 3, StringValue "c", BoolValue True]}
    it "Parses correct select now() statement " $ do
     Lib3.parseStatement2 "select now();" `shouldBe` Right (Lib3.SelectNow )
    it "Parses correct SELECT NOW() statement with UPPERCASE " $ do
     Lib3.parseStatement2 "SELECT NOW();" `shouldBe` Right (Lib3.SelectNow )
    it "Parses update statement without conditions" $ do
      Lib3.parseStatement2 "update flags set flag = 'a';" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "a"))], Lib3.selectWhere = Nothing}
    it "Fails to parse update statement without table name" $ do
      Lib3.parseStatement2 "update  set flag = 'a';" `shouldSatisfy` isLeft
    it "Parses update statement with condition" $ do
      Lib3.parseStatement2 "update flags set flag = 'a' where flag = 'b';" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "a"))], Lib3.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses update statement with multiple conditions" $ do
      Lib3.parseStatement2 "update flags set flag = 'a' where flag = 'b' and id = 2;" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "a"))], Lib3.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b")),Condition (ColumnOperand "id") IsEqualTo  (ConstantOperand (IntegerValue 2))]}
    -- it "Parses drop table statement"
    --   Lib3.ParsedStatement2 "drop table flags;" `shouldBe` Right Lib3.DropTable {Lib3.table = "flags"}
    -- it "Parses create table statement"
    --   Lib3.ParsedStatement2 "create table flags (column1 int, column2 varchar);" `shouldBe` Right Lib3.CreateTable {Lib3.table = "flags", Lib3.newColumns = ["column1","column2"]}
  describe "Lib3.deserializedContent" $ do
    it "parses valid tables" $ do
      Lib3.deserializedContent  "{\
  \  \"Table\":\"employees\",\
  \  \"Columns\":[\
  \    {\"Name\":\"id\", \"ColumnType\":\"IntegerType\"},\
  \    {\"Name\":\"name\", \"ColumnType\":\"StringType\"},\
  \    {\"Name\":\"surname\", \"ColumnType\":\"StringType\"}\
  \  ],\
  \  \"Rows\":[\
  \    {\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue Vi\"},{\"Value\":\"StringValue Po\"}]},\
  \    {\"Row\":[{\"Value\":\"IntegerValue 2\"},{\"Value\":\"StringValue Ed\"},{\"Value\":\"StringValue Dl\"}]}\
  \  ]\
  \}" `shouldBe` Right ("employees", DataFrame [ Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [ [IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]] )
    it "doesnt parse invalid tables" $ do
        Lib3.deserializedContent "{\
  \  \"Table\":\"employees\",\
  \  \"Columns\":[\
  \    {\"Name\":\"id\", \"ColumnType\":\"IntegerType\"},\
  \    {\"Name\":\"name\", \"ColumnType\":\"StringType\"},\
  \    {\"Name\":\"surname\", \"ColumnType\":\"StringType\"}\
  \  ],\
  \  \"Rows\":[\
  \    {\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue Vi\"},{\"Value\":\"StringValue Po\"}]},\
  \    {\"Row\":[{\" Dl\"}]}\
  \  ]\
  \}" `shouldSatisfy` isLeft
    it "parses tables with no rows" $ do
          Lib3.deserializedContent "{\
  \  \"Table\":\"employees\",\
  \  \"Columns\":[\
  \    {\"Name\":\"id\", \"ColumnType\":\"IntegerType\"},\
  \    {\"Name\":\"name\", \"ColumnType\":\"StringType\"},\
  \    {\"Name\":\"surname\", \"ColumnType\":\"StringType\"}\
  \  ],\
  \  \"Rows\":[ ]\
  \}" `shouldBe` Right ("employees", DataFrame [ Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [] )
    it "parses table with no columns" $ do
        Lib3.deserializedContent "{\
  \  \"Table\":\"employees\",\
  \  \"Columns\":[],\
  \  \"Rows\":[]\
  \}" `shouldBe` Right ("employees", DataFrame [] [])
  describe "Lib3.serializedContent" $ do
    it "serializes table with no columns" $ do
        Lib3.serializedContent
          ("employees", DataFrame [ Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [ [IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]] )
          `shouldBe` Right "{\"Table\":\"employees\",\"Columns\":[{\"Name\":\"id\",\"ColumnType\":\"IntegerType\"},{\"Name\":\"name\",\"ColumnType\":\"StringType\"},{\"Name\":\"surname\",\"ColumnType\":\"StringType\"}],\"Rows\":[{\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue Vi\"},{\"Value\":\"StringValue Po\"}]},{\"Row\":[{\"Value\":\"IntegerValue 2\"},{\"Value\":\"StringValue Ed\"},{\"Value\":\"StringValue Dl\"}]}]}"
    it "serializes tables with no rows" $ do
        Lib3.serializedContent ("employees", DataFrame [ Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [] )
        `shouldBe` Right "{\
  \\"Table\":\"employees\",\
  \\"Columns\":[\
  \{\"Name\":\"id\",\"ColumnType\":\"IntegerType\"},\
  \{\"Name\":\"name\",\"ColumnType\":\"StringType\"},\
  \{\"Name\":\"surname\",\"ColumnType\":\"StringType\"}\
  \],\
  \\"Rows\":[]\
  \}"
    it "writes correct tables" $ do
      Lib3.serializedContent ("employees", DataFrame [ Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [ [IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]] )
        `shouldBe` Right "{\
  \\"Table\":\"employees\",\
  \\"Columns\":[\
  \{\"Name\":\"id\",\"ColumnType\":\"IntegerType\"},\
  \{\"Name\":\"name\",\"ColumnType\":\"StringType\"},\
  \{\"Name\":\"surname\",\"ColumnType\":\"StringType\"}\
  \],\
  \\"Rows\":[\
  \{\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue Vi\"},{\"Value\":\"StringValue Po\"}]},\
  \{\"Row\":[{\"Value\":\"IntegerValue 2\"},{\"Value\":\"StringValue Ed\"},{\"Value\":\"StringValue Dl\"}]}\
  \]\
  \}"
  describe "Lib3.parseStatement2" $ do
    it "shows the right table stored in db" $ do
      Lib3.parseStatement2 "show table employees;" `shouldBe` Right (Lib3.ShowTable "employees") 
    it "shows the list of tables which are stored in db" $ do
      Lib3.parseStatement2 "show tables;" `shouldBe` Right Lib3.ShowTables
    it "Parses correct update statements" $ do
      Lib3.parseStatement2 "update flags set flag = 'c', value = True where flag = 'b';" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "c")), Condition (ColumnOperand "value") IsEqualTo  (ConstantOperand (BoolValue True))], Lib3.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses correct insert statements" $ do
      Lib3.parseStatement2 "insert into flags (flag) values ('b');" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Just (Lib3.ColumnsSelected ["flag"]), Lib3.values = [StringValue "b"]}
    it "Parses correct delete statements" $ do
      Lib3.parseStatement2 "delete from flags where flag = 'b';" `shouldBe` Right Lib3.Delete {Lib3.table = "flags", Lib3.conditions = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Does not parse incorrect delete statements" $ do
      Lib3.parseStatement2 "delete from flags where flag =" `shouldSatisfy` isLeft
    it "Does not parse incorrect insert statements" $ do
      Lib3.parseStatement2 "insert into flags (flag)ag =" `shouldSatisfy` isLeft
    it "Does not parse incorrect update statements" $ do
      Lib3.parseStatement2 "update flags set flag = 'c', value = True whag)ag =" `shouldSatisfy` isLeft
  describe "Lib3.MockedDatabase" $ do  
    it "Checking simple SELECT" $ do
      db <- dataBase
      res <- runExecuteIO' db getCurrentTime $ Lib3.executeSql "select id from flags;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1],[IntegerValue 1],[IntegerValue 2],[IntegerValue 2]])
    it "Checking SHOW TABLES query" $ do
      db <- dataBase
      res <- runExecuteIO' db getCurrentTime $ Lib3.executeSql "show tables;"
      res `shouldBe` Right (DataFrame [Column "Tables" StringType] [[StringValue "employees"],[StringValue "flags"]])
    it "Checking SHOW TABLE query" $ do
      db <- dataBase
      res <- runExecuteIO' db getCurrentTime $ Lib3.executeSql "show table employees;"
      res `shouldBe` Right (DataFrame [Column "employees" StringType] [[StringValue "id"],[StringValue "name"],[StringValue "surname"]])
    it "Checking SELECT NOW() query" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select now();"
      res `shouldBe` Right (DataFrame [Column "Now" StringType] [[StringValue "2023-11-27 01:59:59"]]) --Musu laiko zonoj prideda 2h
    it "Checking SELECT NOW() query with column names" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select now(), id from employees;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "Now" StringType] [[IntegerValue 1, StringValue "2023-11-27 01:59:59"], [IntegerValue 2, StringValue "2023-11-27 01:59:59"]])
    it "Checking SELECT NOW() query with aggregates" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select now(), sum(employees.id), max(value) from employees, flags where flag = 'b';"
      res `shouldBe` Right (DataFrame [Column "Max value" BoolType, Column "Sum employees.id" IntegerType, Column "Now" StringType] [[BoolValue True, IntegerValue 9, StringValue "2023-11-27 01:59:59"]])
    it "Checking UPDATE query" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "update flags set value = True where id = 2;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType] [[IntegerValue 1, StringValue "a", BoolValue True],[IntegerValue 1, StringValue "b", BoolValue True],[IntegerValue 2, StringValue "b", BoolValue True],[IntegerValue 2, StringValue "b", BoolValue True]])
    it "Checking UPDATE query with multiple sets and conditions" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "update flags set value = True, flag = 'a' where id = 2 and flag = 'b';"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType] [[IntegerValue 1, StringValue "a", BoolValue True],[IntegerValue 1, StringValue "a", BoolValue True],[IntegerValue 2, StringValue "a", BoolValue True],[IntegerValue 2, StringValue "a", BoolValue True]])
    it "Checking INSERT INTO query" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "insert into flags (flag) values ('b');"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType] [[IntegerValue 1, StringValue "a", BoolValue True], [IntegerValue 1, StringValue "b", BoolValue True], [IntegerValue 2, StringValue "b", NullValue], [IntegerValue 2, StringValue "b", BoolValue False], [NullValue, StringValue "b", NullValue]])
    it "Checking INSERT INTO query with multiple columns and multiple values" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "insert into flags (flag, value) values ('b', False);"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType] [[IntegerValue 1, StringValue "a", BoolValue True], [IntegerValue 1, StringValue "b", BoolValue True], [IntegerValue 2, StringValue "b", NullValue], [IntegerValue 2, StringValue "b", BoolValue False], [NullValue, StringValue "b", BoolValue False]])
    it "Checking DELETE FROM query without conditions" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "delete from flags;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType] [])    
    it "Checking DELETE FROM query with multiple conditions" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "delete from flags where value = True and flag = 'b';"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType] [[IntegerValue 1, StringValue "a", BoolValue True],[IntegerValue 2, StringValue "b", NullValue],[IntegerValue 2, StringValue "b", BoolValue False]])
    it "Checking SELECT ALL query with multiple tables" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select * from flags, employees;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "flag" StringType, Column "value" BoolType, Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "a", BoolValue True, IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 1, StringValue "a", BoolValue True, IntegerValue 2, StringValue "Ed", StringValue "Dl"], [IntegerValue 1, StringValue "b", BoolValue True, IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 1, StringValue "b", BoolValue True, IntegerValue 2, StringValue "Ed", StringValue "Dl"], [IntegerValue 2, StringValue "b", NullValue, IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "b", NullValue, IntegerValue 2, StringValue "Ed", StringValue "Dl"], [IntegerValue 2, StringValue "b", BoolValue False, IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "b", BoolValue False, IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    it "Checking SELECT ALL query with multiple tables and multiple conditions" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select * from flags, employees where flag = 'a' and employees.id = 2;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType,Column "flag" StringType,Column "value" BoolType,Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "a",BoolValue True,IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "Checking SELECT ALL query with multiple tables and columns with provided table names" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select * from flags, employees where flags.id = employees.id;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType,Column "flag" StringType,Column "value" BoolType,Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "a",BoolValue True,IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 1,StringValue "b",BoolValue True,IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "b",NullValue,IntegerValue 2,StringValue "Ed",StringValue "Dl"],[IntegerValue 2,StringValue "b",BoolValue False,IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "Checking if column names provided in conditions are ambiguous" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select * from flags, employees where id = 2;"
      res `shouldSatisfy` isLeft 
    it "Checking if table name with column name provided in condition exists" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select * from flags, employees where miegas.value = False;"
      res `shouldSatisfy` isLeft 
    it "Checking SELECT query with multiple tables" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select flag from flags, employees;"
      res `shouldBe` Right (DataFrame [Column "flag" StringType] [[StringValue "a"],[StringValue "a"],[StringValue "b"],[StringValue "b"],[StringValue "b"],[StringValue "b"],[StringValue "b"],[StringValue "b"]])
    it "Checking SELECT query with multiple tables and multiple columns" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select flag, value from flags, employees;"
      res `shouldBe` Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "a", BoolValue True], [StringValue "a", BoolValue True], [StringValue "b", BoolValue True], [StringValue "b", BoolValue True], [StringValue "b", NullValue], [StringValue "b", NullValue], [StringValue "b", BoolValue False], [StringValue "b", BoolValue False]])
    it "Checking SELECT query with multiple tables, multiple columns with provided table names and multiple conditions" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select flags.value, employees.id from flags, employees where name = 'Vi';"
      res `shouldBe` Right (DataFrame [Column "value" BoolType,Column "id" IntegerType] [[BoolValue True,IntegerValue 1],[BoolValue True,IntegerValue 1],[NullValue,IntegerValue 1],[BoolValue False,IntegerValue 1]])
    it "Checking SELECT query with multiple tables and multiple aggregation functions and multiple conditions" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select max(flag), sum(employees.id) from flags, employees where value = True and surname = 'Dl';"
      res `shouldBe` Right (DataFrame [Column "Max flag" StringType,Column "Sum employees.id" IntegerType] [[StringValue "b",IntegerValue 4]])   
    it "Checking if provided column names are ambiguous" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select id from flags, employees;"
      res `shouldSatisfy` isLeft 
    it "Checking if provided table name with column name exists" $ do
      db <- dataBase
      res <- runExecuteIO' db testCurrentTime $ Lib3.executeSql "select miegas.id from flags, employees;"
      res `shouldSatisfy` isLeft 
  describe "Lib4.parseStatement" $ do   
    it "Parses correct select now() statement " $ do
     Lib4.parseStatement "select now();" `shouldBe` Right (Lib4.SelectNow )
    it "Parses correct SELECT NOW() statement with UPPERCASE " $ do
     Lib4.parseStatement "SELECT NOW();" `shouldBe` Right (Lib4.SelectNow )
    it "Parses correct drop table statement " $ do
     Lib4.parseStatement "drop table flags;" `shouldBe` Right (Lib4.DropTable{Lib4.table = "flags"} )
    it "Parses correct drop table statement with UPPERCASE " $ do
     Lib4.parseStatement "DROP TABLE flags;" `shouldBe` Right (Lib4.DropTable{Lib4.table = "flags"} )
    it "Does not execute create table statement when no columns created " $ do
     Lib4.parseStatement "create table flags();" `shouldSatisfy` isLeft 
    it "Parses correct create table statement " $ do
     Lib4.parseStatement "create table flags (column1 varchar);" `shouldBe` Right (Lib4.CreateTable{Lib4.table = "flags", Lib4.newColumns = [Column "column1" StringType ]} )
    it "Parses correct show table statement" $ do
     Lib4.parseStatement "show table employees;" `shouldBe` Right (Lib4.ShowTable "employees") 
    it "Parses correct show tables statement " $ do
     Lib4.parseStatement "show tables;" `shouldBe` Right Lib4.ShowTables
    it "Parses correct update statements" $ do
     Lib4.parseStatement "update flags set flag = 'c', value = True where flag = 'b';" `shouldBe` Right Lib4.Update {Lib4.table = "flags", Lib4.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "c")), Condition (ColumnOperand "value") IsEqualTo  (ConstantOperand (BoolValue True))], Lib4.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses correct insert statements" $ do
     Lib4.parseStatement "insert into flags (flag) values ('b');" `shouldBe` Right Lib4.Insert {Lib4.table = "flags", Lib4.columns = Just (Lib4.ColumnsSelected ["flag"]), Lib4.values = [StringValue "b"]}
    it "Parses correct delete statements" $ do
     Lib4.parseStatement "delete from flags where flag = 'b';" `shouldBe` Right Lib4.Delete {Lib4.table = "flags", Lib4.conditions = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Does not parse incorrect delete statements" $ do
     Lib4.parseStatement "delete from flags where flag =" `shouldSatisfy` isLeft
    it "Does not parse incorrect insert statements" $ do
     Lib4.parseStatement "insert into flags (flag)ag =" `shouldSatisfy` isLeft
    it "Does not parse incorrect update statements" $ do
     Lib4.parseStatement "update flags set flag = 'c', value = True whag)ag =" `shouldSatisfy` isLeft
    it "Parses the right column from one table" $ do
     Lib4.parseStatement "select id from employees;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectColumn2 ["id"](Nothing), Lib4.tables = ["employees"], Lib4.selectWhere = Nothing, Lib4.order = Nothing} 
    it "Parses  multiple columns from multiple tables" $ do
     Lib4.parseStatement "select id, name from employees, flags;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectColumn2 ["id", "name"](Nothing), Lib4.tables = ["employees", "flags"], Lib4.selectWhere = Nothing, Lib4.order = Nothing}
    it "Parses the max of the specified column from the specified table" $ do
     Lib4.parseStatement "select max(id) from employees;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectAggregate2 [Lib4.AggregateColumn (Max, "id")](Nothing), Lib4.tables = ["employees"], Lib4.selectWhere = Nothing, Lib4.order = Nothing};
    it "Parses multiple aggregate functions from the specified table" $ do
     Lib4.parseStatement "select max(id), sum(id) from employees;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectAggregate2 [Lib4.AggregateColumn (Max, "id"), Lib4.AggregateColumn (Sum, "id")](Nothing), Lib4.tables = ["employees"], Lib4.selectWhere = Nothing, Lib4.order = Nothing};
    it "Parses the correct query with simple where conditions with multiple tables" $ do
     Lib4.parseStatement "select id,name from employees, flags where flag.id = 1;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectColumn2 ["id", "name"](Nothing), Lib4.tables = ["employees", "flags"], Lib4.selectWhere  = Just[ Condition (ColumnTableOperand("flag", "id")) IsEqualTo  (ConstantOperand (IntegerValue 1)) ], Lib4.order = Nothing}
    it "Parses the correct query with where conditions that are not valid with provided operator with multiple tables" $ do
     Lib4.parseStatement "select flags.id from employees, flags where employees.name > employees.surname;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectedColumnsTables [("flags", "id")](Nothing), Lib4.tables = ["employees", "flags"], Lib4.selectWhere = Just[ Condition (ColumnTableOperand("employees", "name")) IsGreaterThan  (ColumnTableOperand("employees", "surname")) ], Lib4.order = Nothing}
    it "Parses simple order by statement" $ do
     Lib4.parseStatement "select id from flag order by id asc;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectColumn2["id"](Nothing), Lib4.tables = ["flag"], Lib4.selectWhere = Nothing, Lib4.order = Just [(Lib4.ColumnName "id", Asc "asc")]}
    it "Parses  order by statement with integers" $ do
     Lib4.parseStatement "select id from flag order by 1 desc;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = Lib4.SelectColumn2["id"](Nothing), Lib4.tables = ["flag"], Lib4.selectWhere = Nothing, Lib4.order = Just [(ColumnNumber 1, Desc "desc")]}
    it "Parses  order by statement with more than 1 order condition" $ do
     Lib4.parseStatement "select id, value from flags order by 1 desc, 2 asc;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = SelectColumn2["id", "value"](Nothing), Lib4.tables = ["flags"], Lib4.selectWhere = Nothing, Lib4.order = Just [(ColumnNumber 1, Desc "desc"),(ColumnNumber  2, Asc "asc")]}
    it "Parses  order by statement with where statement" $ do
     Lib4.parseStatement "select value, id from flags where id = 2 order by value asc;" `shouldBe` Right Lib4.Select {Lib4.selectQuery = SelectColumn2["value", "id"](Nothing), Lib4.tables = ["flags"], Lib4.selectWhere = Just[ Condition (ColumnOperand "id") IsEqualTo  (ConstantOperand (IntegerValue 2)) ], Lib4.order = Just [(Lib4.ColumnName "value", Asc "asc")]}
    it "Does not execute incorrect order by statement" $ do
     Lib4.parseStatement "select id from flag order id asc ;" `shouldSatisfy` isLeft
    it "Parses  order by statement with select all statement" $ do
     Lib4.parseStatement "select * from flag, id order by 1 asc;" `shouldBe` Right Lib4.SelectAll {Lib4.tables = ["flag", "id"], Lib4.selectWhere = Nothing, Lib4.order = Just [(ColumnNumber 1, Asc "asc")]}
    it "Parses  order by statement with select all statement with conditions" $ do
     Lib4.parseStatement "select * from flag, id where flag.value = 2 order by 1 asc;" `shouldBe` Right Lib4.SelectAll {Lib4.tables = ["flag", "id"], Lib4.selectWhere = Just[Condition (ColumnTableOperand("flag", "value")) IsEqualTo (ConstantOperand (IntegerValue 2))], Lib4.order = Just [(ColumnNumber 1, Asc "asc")]}
  describe "Execution of lib4" $ do 
    it "Deletes the specifed table" $ do
      db <- dataBase
      res <- runExecuteIO4 db testCurrentTime $ Lib4.executeSql "drop table flags;"
      res `shouldSatisfy` isRight
    it "Acts accordingly when there is no table wanted to delete" $ do
      db <- dataBase
      res <- runExecuteIO4 db testCurrentTime $ Lib4.executeSql "drop table ananasas;"
      res `shouldSatisfy` isLeft
    it "Creates a table" $ do
      db <- dataBase
      res <- runExecuteIO4 db testCurrentTime $ Lib4.executeSql "create table cars (car int);"
      res `shouldBe` Right (DataFrame [Column "car" IntegerType] [])
    it "Creates a table" $ do
      db <- dataBase
      res <- runExecuteIO4 db testCurrentTime $ Lib4.executeSql "create table test (test varchar, testing int, tests bool);"
      res `shouldBe` Right (DataFrame [Column "test" StringType, Column "testing" IntegerType, Column "tests" BoolType] [])
    it "Returns the ordered table" $ do
      db <- dataBase
      res <- runExecuteIO4 db testCurrentTime $ Lib4.executeSql "select id, name from employees order by id;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType,Column "name" StringType] [[IntegerValue 1,StringValue "Vi"],[IntegerValue 2,StringValue "Ed"]])
    it "Returns the ordered table" $ do
      db <- dataBase
      res <- runExecuteIO4 db testCurrentTime $ Lib4.executeSql "select * from employees order by id, name;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])

type Database =  [(String, IORef String)]

dataBase :: IO Main.Database
dataBase = do
  employees <- newIORef "{\
    \\"Table\":\"employees\",\
    \\"Columns\":[\
    \{\"Name\":\"id\",\"ColumnType\":\"IntegerType\"},\
    \{\"Name\":\"name\",\"ColumnType\":\"StringType\"},\
    \{\"Name\":\"surname\",\"ColumnType\":\"StringType\"}\
    \],\
    \\"Rows\":[\
    \{\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue Vi\"},{\"Value\":\"StringValue Po\"}]},\
    \{\"Row\":[{\"Value\":\"IntegerValue 2\"},{\"Value\":\"StringValue Ed\"},{\"Value\":\"StringValue Dl\"}]}\
    \]\
    \}"
  flags <- newIORef "{\
    \\"Table\":\"flags\",\
    \\"Columns\":[\
    \{\"Name\":\"id\",\"ColumnType\":\"IntegerType\"},\
    \{\"Name\":\"flag\",\"ColumnType\":\"StringType\"},\
    \{\"Name\":\"value\",\"ColumnType\":\"BoolType\"}\
    \],\
    \\"Rows\":[\
    \{\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue a\"},{\"Value\":\"BoolValue True\"}]},\
    \{\"Row\":[{\"Value\":\"IntegerValue 1\"},{\"Value\":\"StringValue b\"},{\"Value\":\"BoolValue True\"}]},\
    \{\"Row\":[{\"Value\":\"IntegerValue 2\"},{\"Value\":\"StringValue b\"},{\"Value\":\"NullValue\"}]},\
    \{\"Row\":[{\"Value\":\"IntegerValue 2\"},{\"Value\":\"StringValue b\"},{\"Value\":\"BoolValue False\"}]}\
    \]\
    \}"
  return [("employees", employees),("flags", flags)]

testCurrentTime :: IO UTCTime
testCurrentTime = return $ read "2023-11-26 23:59:59 UTC"


runExecuteIO :: Main.Database -> Lib3.Execution r -> IO r
runExecuteIO _ (Pure r) = return r
runExecuteIO table (Free step) = do
    next <- runStep table step
    runExecuteIO table (snd next)
  where
    runStep :: Main.Database -> Lib3.ExecutionAlgebra a -> IO (Main.Database, a)
    runStep table (Lib3.GetTime next) = do
        currentTime <- testCurrentTime
        return (table, next currentTime)
    runStep table (Lib3.LoadFile tableName next) = case lookup tableName table of
        Just ref -> readIORef ref >>= \content -> return (table, next $ Lib3.deserializedContent content)
        Nothing -> return (table, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")

runExecuteIO' :: Main.Database -> IO UTCTime -> Lib3.Execution r -> IO r
runExecuteIO' table time (Pure r) = return r
runExecuteIO' table time (Free step) = do
    (dataB, next) <- runStep table time step
    runExecuteIO' dataB time next

runStep :: Main.Database -> IO UTCTime -> Lib3.ExecutionAlgebra a -> IO (Main.Database, a)
runStep table time (Lib3.GetTime next) = do
    currentTime <- time
    return (table, next currentTime)

runStep table _ (Lib3.LoadFile tableName next) =
    case lookup tableName table of
        Just ref -> readIORef ref >>= \content -> return (table, next $ Lib3.deserializedContent content)
        Nothing -> return (table, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")

runStep db _ (Lib3.SaveFile (tableName, tableContent) next) =
    case lookup tableName db of
        Just ref -> case Lib3.serializedContent (tableName, tableContent) of
            Left err -> error err
            Right serializedTable -> do
                writeIORef ref serializedTable
                return (db, next ())
        Nothing -> return (db, next ())

runStep db _ (Lib3.GetTables next) =
    return (db, next $ map fst db)

runExecuteIO4 :: Main.Database -> IO UTCTime -> Lib4.Execution r -> IO r
runExecuteIO4 table time (Pure r) = return r
runExecuteIO4 table time (Free step) = do
    (dataB, next) <- runStep4 table time step
    runExecuteIO4 dataB time next


runStep4 :: Main.Database -> IO UTCTime -> Lib4.ExecutionAlgebra a -> IO (Main.Database, a)
runStep4 db time (Lib4.GetTime next) = do
    currentTime <- time
    return (db, next currentTime)
runStep4 db _ (Lib4.DropFile tableName next) =
    case lookup tableName db of
        Just ref -> do
          let newDb = filter (\(name, _) -> name /= tableName) db
          return (newDb, next $ Right ())
        Nothing -> return (db, next $ Left $ "Table not found.")
runStep4 db _ (Lib4.SaveFile (tableName, tableContent) next) =
        case lookup tableName db of
            Just ref -> case Lib3.serializedContent (tableName, tableContent) of
                Left err -> error err  
                Right serializedTable -> do
                    writeIORef ref serializedTable
                    return (db, next ())
            Nothing -> return (db, next ())  
runStep4 db _ (Lib4.LoadFile tableName next) =
        case lookup tableName db of
            Just ref -> readIORef ref >>= \content -> return (db, next $ Lib3.deserializedContent content)
            Nothing -> return (db, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")