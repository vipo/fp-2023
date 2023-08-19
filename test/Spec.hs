import Data.Either
import Data.Maybe ()
import Lib1
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName Lib1.inMemoryDatabase "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName Lib1.inMemoryDatabase "employees" `shouldBe` Just employeesTable
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName Lib1.inMemoryDatabase "employEEs" `shouldBe` Just employeesTable
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame tableInvalid1 `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame tableInvalid2 `shouldSatisfy` isLeft
    it "passes valid tables" $ do
      Lib1.validateDataFrame tableInvalid2 `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 employeesTable `shouldSatisfy` not . null
  where
    employeesTable = snd (head Lib1.inMemoryDatabase)
    tableInvalid1 = snd (Lib1.inMemoryDatabase !! 1)
    tableInvalid2 = snd (Lib1.inMemoryDatabase !! 1)
