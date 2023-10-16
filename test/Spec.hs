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
    it "handles empty input" $ do
      Lib2.parseStatement "" `shouldSatisfy` isLeft
    it "handles unexpected symbols after end of statement" $ do
      Lib2.parseStatement "SHOW TABLE Users;a" `shouldSatisfy` isLeft
    it "handles whitespace error" $ do
      Lib2.parseStatement "ShowTable Users;" `shouldSatisfy` isLeft
    it "handles show table without table name" $ do
      Lib2.parseStatement "SHOW TABLE" `shouldSatisfy` isLeft
    it "parses show table statement with uppercase" $ do
      Lib2.parseStatement "SHOW TABLE Users;" `shouldBe` Right (ShowTableStatement "Users")
    it "parses show table statement with lowercase alphanum and underscore table name" $ do
      Lib2.parseStatement "show table _organization_123" `shouldBe` Right (ShowTableStatement "_organization_123")
    it "parses show table statement with mixed casing and whitespace" $ do
      Lib2.parseStatement "   ShOW   taBlE    Hello_WORLD   ;  " `shouldBe` Right (ShowTableStatement "Hello_WORLD")
