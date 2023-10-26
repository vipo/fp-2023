import Data.Either
import Data.Maybe ()
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
      --reikes sutvarkyti kai bus pilnas konstruktorius su where 
    it "shows the right column" $ do
      Lib2.parseStatement "select id from employees;" `shouldSatisfy` isRight
    it "shows right multiple columns" $ do
      Lib2.parseStatement "select id, name from employees;" `shouldSatisfy` isRight
    it "shows the list of tables" $ do
      Lib2.parseStatement "select id, name from employees;" `shouldSatisfy` isRight
    it "doesn't execute queries without a ';'" $ do
      Lib2.parseStatement "select id, name from employees" `shouldSatisfy` isLeft
    it "doesn't execute queries with fake columns" $ do
      Lib2.parseStatement "select vardas from employees;" `shouldSatisfy` isLeft
    it "doesn't execute queries with existing columns and fake tables" $ do
      Lib2.parseStatement "select id from darbuotojai;" `shouldSatisfy` isLeft
    it "shows the max of the specified column from the specified table" $ do
      Lib2.parseStatement "select max(id) from employees" `shouldSatisfy` isRight
    it "shows multiple aggregate functions" $ do
      Lib2.parseStatement "select max(id), sum(id) from employees" `shouldSatisfy` isRight