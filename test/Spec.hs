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
import qualified Lib3
import qualified Lib3
import qualified Lib3

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
    it "Parses correct update statements" $ do
      Lib3.parseStatement2 "update flags set flag = 'c', value = True where flag = 'b';" `shouldBe` Right Lib3.Update {Lib3.table = "flags", Lib3.selectUpdate = [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "c")), Condition (ColumnOperand "value") IsEqualTo  (ConstantOperand (BoolValue True))], Lib3.selectWhere = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Parses correct insert statements" $ do
      Lib3.parseStatement2 "insert into flags (flag) values ('b');" `shouldBe` Right Lib3.Insert {Lib3.table = "flags", Lib3.columns = Just (ColumnsSelected ["flag"]), Lib3.values = [StringValue "b"]}
    it "Parses correct delete statements" $ do
      Lib3.parseStatement2 "delete from flags where flag = 'b';" `shouldBe` Right Lib3.Delete {Lib3.table = "flags", Lib3.conditions = Just [Condition (ColumnOperand "flag") IsEqualTo  (ConstantOperand (StringValue "b"))]}
    it "Does not parse incorrect delete statements" $ do
      Lib3.parseStatement2 "delete from flags where flag =" `shouldSatisfy` isLeft
    it "Does not parse incorrect insert statements" $ do
      Lib3.parseStatement2 "insert into flags (flag)ag =" `shouldSatisfy` isLeft
    it "Does not parse incorrect update statements" $ do
      Lib3.parseStatement2 "update flags set flag = 'c', value = True whag)ag =" `shouldSatisfy` isLeft



type Database =  [(String, IORef String)]


dataBase :: IO Database
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
  \{\"Name\":\"id\", \"ColumnType\":\"IntegerType\"},\
  \{\"Name\":\"flag\", \"ColumnType\":\"StringType\"},\
  \{\"Name\":\"value\", \"ColumnType\":\"BoolType\"}\
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


-- runExecuteIO :: Database -> Lib3.Execution r -> IO r
-- runExecuteIO _ (Pure r) = return r
-- runExecuteIO table (Free step) = do
--     next <- runStep table step
--     runExecuteIO table next 
--   where
--     runStep :: Database -> Lib3.ExecutionAlgebra a -> IO (Database, a)
--     runStep table (Lib3.GetTime next) = return (table, next testCurrentTime)
--     -- runStep table (Lib3.LoadFile tableName next) = case lookup tableName table of
--     --         Just ref -> readIORef ref >>= \content -> return (db, next $ Lib3.deserializedContent content)
--     --         Nothing -> return (db, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")



-- runExecuteIO :: Database -> IO UTCTime -> Lib3.Execution r -> IO r
-- runExecuteIO table time (Pure r) = return r
-- runExecuteIO table time (Free step) = do
--     (dataB, next) <- runStep databBase step
--     runExecuteIO dataB time next
--   where
--     runStep :: Database -> IO UTCTime -> Lib3.ExecutionAlgebra a -> IO a
--     runStep table times (Lib3.GetTime next) = 
--         getCurrentTime >>= \time -> return (db, next time)

--     runStep table (Lib3.LoadFile tableName next) =
--         case lookup tableName table of
--             Just ref -> readIORef ref >>= \content -> return (db, next $ Lib3.deserializedContent content)
--             Nothing -> return (db, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")

--     runStep db (Lib3.SaveFile (tableName, tableContent) next) =
--         case lookup tableName db of
--             Just ref -> case Lib3.serializedContent (tableName, tableContent) of
--                 Left err -> error err  
--                 Right serializedTable -> do
--                     writeIORef ref serializedTable
--                     return (db, next ())
--             Nothing -> return (db, next ())  

--     runStep db getCurrentTime' (Lib3.GetTables next) = 
--         return (db, next $ map fst db)


