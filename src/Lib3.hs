{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use <$>" #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE EmptyCase #-}
{-# LANGUAGE BlockArguments #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# HLINT ignore "Redundant pure" #-}
{-# LANGUAGE OverloadedStrings #-}
{-# HLINT ignore "Use if" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    deserializedContent,
    serializedContent,
    SelectedColumns(..),
    NowFunction,
    FromJSONValue,
    DeserializedContent,
    CartesianColumn(..),
    CartesianDataFrame(..),
    UTCTime,
    ParsedStatement2(..),
    parseStatement2,
    WhereSelect,
    TableName,
    SelectedColumns,
    InsertedValues,
    SpecialSelect,
    SelectedColumns,
    TableArray

  )
where
import Lib2
import Lib1 (renderDataFrameAsTable, findTableByName, parseSelectAllStatement, checkTupleMatch, zipColumnsAndValues, checkRowSizes, validateDataFrame)
import Control.Monad.Free (Free (..), liftF)
import Data.Time
import DataFrame as DF
import Data.Either (fromRight)
import Data.List (find, findIndex, elemIndex, nub, elem, intercalate)
import Text.ParserCombinators.ReadP (many1, sepBy1)
import GHC.Generics
import GHC.Base (VecElem(DoubleElemRep))
import Debug.Trace (trace, traceShow)
import Data.Char (isDigit)
import Data.Maybe (fromMaybe)
import Data.Time.Clock (UTCTime)
import Data.Aeson
import Control.Monad
import GHC.Base (VecElem(DoubleElemRep))
import qualified Data.ByteString.Lazy.Char8 as BS
import Text.Read (readMaybe)


type TableName = String
type FileContent = String
type DeserializedContent = (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data NowFunction = Now
  deriving (Show, Eq)

data Aggregate2 = AggregateColumn (AggregateFunction, ColumnName) | AggregateColumnTable (AggregateFunction, (TableName, ColumnName))
  deriving (Show, Eq)

data SpecialSelect2 = SelectAggregate2 [Aggregate2] (Maybe NowFunction) | SelectColumn2 [ColumnName] (Maybe NowFunction) | SelectedColumnsTables [(TableName, ColumnName)] (Maybe NowFunction)
  deriving (Show, Eq)

data CartesianColumn = CartesianColumn (TableName, Column)
  deriving (Show, Eq) 

data CartesianDataFrame = CartesianDataFrame [CartesianColumn] [Row]
  deriving (Show, Eq)

data ExecutionAlgebra next =
    GetTables (TableArray -> next)
  | LoadFile TableName (Either ErrorMessage DeserializedContent -> next)
  | SaveFile  (TableName, DataFrame) (() -> next)
  | GetTime (UTCTime -> next)
  | DropFile TableName (Maybe ErrorMessage -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

------------------------------- data -----------------------------------

data FromJSONTable = FromJSONTable {
    deserializedtableName :: String,
    deserializedColumns :: [FromJSONColumn],
    deserializedRows :: [FromJSONRow]
} deriving (Show, Eq, Generic)

data FromJSONColumn = FromJSONColumn {
    deserializedName :: String,
    deserializedDataType :: String
} deriving (Show, Eq, Generic)

data FromJSONValue = FromJSONValue {
    deserializedValue :: String
} deriving (Show, Eq, Generic)

data FromJSONRow = FromJSONRow {
    deserializedRow :: [FromJSONValue]
} deriving (Show, Eq, Generic)


----------------------------- instances ----------------------------------

instance FromJSON FromJSONColumn where
  parseJSON (Object v) =
    FromJSONColumn <$> v .: "Name"
                   <*> v .: "ColumnType"
  parseJSON _ = mzero

instance FromJSON FromJSONTable where
  parseJSON (Object v) =
    FromJSONTable <$> v .: "Table"
                  <*> v .: "Columns"
                  <*> v .: "Rows"
  parseJSON _ = mzero

instance FromJSON FromJSONValue where
  parseJSON (Object v) =
    FromJSONValue <$> v .: "Value"
  parseJSON _ = mzero

instance FromJSON FromJSONRow where
  parseJSON (Object v) =
    FromJSONRow <$> v .: "Row"
  parseJSON _ = mzero

deserializedContent :: FileContent -> Either ErrorMessage (TableName, DataFrame)
deserializedContent json = case (toTable json) of
    Just fromjsontable -> Right (toDataframe fromjsontable)
    Nothing -> Left "Failed to decode JSON"


toTable :: String -> Maybe FromJSONTable
toTable json = decode $ BS.pack json

toDataframe :: FromJSONTable -> (TableName, DataFrame)
toDataframe table =
  (deserializedtableName table ,DataFrame
    (map (\col -> Column (deserializedName col) (toColumnType $ deserializedDataType col)) $ deserializedColumns table)
    (map (map convertToValue . deserializedRow) $ deserializedRows table))

toColumnType :: String -> ColumnType
toColumnType "IntegerType" = IntegerType
toColumnType "StringType"  = StringType
toColumnType "BoolType"    = BoolType
toColumnType _         = error "Unsupported data type"

convertToValue :: FromJSONValue -> DF.Value
convertToValue (FromJSONValue str) =
  case words str of
    ["IntegerValue", val] -> IntegerValue (read val)
    ["StringValue", val] -> StringValue val
    ["BoolValue", "True"] -> BoolValue True
    ["BoolValue", "False"] -> BoolValue False
    ["NullValue"] -> NullValue
    _ -> error "Invalid FromJSONValue format"

----------------------------------------------------------------------------------------------------------------

data ParsedStatement2 =
  SelectNow {}
  | Insert {
    table :: TableName,
    columns :: Maybe SelectedColumns,
    values :: InsertedValues
  }
  | Update {
    table :: TableName,
    selectUpdate :: WhereSelect,
    selectWhere :: Maybe WhereSelect
  }
  | Delete {
    table :: TableName,
    conditions :: Maybe WhereSelect
  }
  | ShowTable {
    table :: TableName
   }
  | SelectAll {
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
   }
  |Select {
    selectQuery :: SpecialSelect2,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
  }
  | ShowTables { }
  | DropTable {
    table :: TableName
  }
  | CreateTable {
    table :: TableName,
    newColumns :: [Column]
  }
    deriving (Show, Eq)

data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [DF.Value]

---------------------------------------------------------------------------

loadFile :: TableName -> Execution (Either ErrorMessage DeserializedContent)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getTables :: Execution TableArray
getTables = liftF $ GetTables id

saveFile :: (TableName, DataFrame) -> Execution ()
saveFile table = liftF $ SaveFile table id

dropFile :: TableName -> Execution (Maybe ErrorMessage)
dropFile table = liftF $ DropFile table id

-----------------------------executeSql queries-----------------------------

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement2 sql of
  Right (Lib3.SelectAll tables selectWhere) -> do
    contents <- loadFromFiles tables
    case contents of
      Right tuples -> case executeSelectAll tables (snd $ switchListToTuple' tuples) selectWhere of
        Right dfs -> return $ Right dfs
        Left err -> return $ Left err
      Left err -> return $ Left err
  Left err -> return $ Left err

  Right (Lib3.ShowTable table) -> do
    content <- loadFile table
    case content of
      Left err -> return $ Left err
      Right content2 -> return $ Right $ executeShowTable (snd content2) table
  Left err -> return $ Left err

  Right (Insert table columns values) -> do
    content <- loadFile table
    case content of
            Left err1 -> return $ Left err1
            Right deserializedContent -> do
              case columns of
                Just columnsProvided -> (insertColumnsProvided deserializedContent columnsProvided values)
                Nothing -> (insertToAll deserializedContent values)

  Right (Update table selectUpdate selectWhere) -> do
    content <- loadFile table
    let condi = [Condition (ConstantOperand (IntegerValue 1)) IsEqualTo (ConstantOperand (IntegerValue 1))]
    case content of
            Left err1 -> return $ Left err1
            Right deserializedContent -> do
              case selectWhere of
                Just selectWhere ->
                    case (filterSelectVol2 (snd deserializedContent) selectUpdate selectWhere) of
                      Right df -> do
                        saveFile (table, df)
                        return $ Right df
                      Left err -> return $ Left err
                Nothing -> case (filterSelectVol2 (snd deserializedContent) selectUpdate condi) of
                  Right df -> do
                        saveFile (table, df)
                        return $ Right df
                  Left err -> return $ Left err

  Right (Delete table conditions) -> do
    content <- loadFile table
    case content of
            Left err1 -> return $ Left err1
            Right deserializedContent -> do
              case (deleteExecution (snd deserializedContent) $ conditions) of
                Right dfs -> do
                        saveFile (table, dfs)
                        return $ Right dfs
                Left err -> return $ Left err
  Right SelectNow -> do
    da <- getTime
    let df = createNowDataFrame (uTCToString da)
    return $ Right df

  Right Lib3.ShowTables -> do 
    files <- getTables
    let df = executeShowTables files
    return $ Right df

  Right (Lib3.Select specialSelect tables selectWhere) -> do
    time <- getTime
    contents <- loadFromFiles tables
    case contents of
      Right tuples -> case executeSelectWithAllSauces specialSelect tables (snd $ switchListToTuple' tuples) selectWhere time of
        Right dfs -> return $ Right dfs
        Left err -> return $ Left err
      Left err -> return $ Left err
  Left err -> return $ Left err

  Right (DropTable table) -> do
        _ <- dropFile table
        return $ Right (DataFrame [] [])

  Right (CreateTable table columns) -> do
        content <- loadFile table
        case content of
            Left _ -> do
                saveFile (table, DataFrame columns [])
                return $ Right (DataFrame columns [])
            Right _ -> return $ Left "This name of table is already used." 
-----------------------------------executeSql queries------------------------------------

--------------------------------------Load FILES-----------------------------------------

loadFromFiles :: TableArray -> Execution (Either ErrorMessage [DeserializedContent])
loadFromFiles tables = do
    results <- traverse loadFile tables
    return $ sequence results

switchListToTuple' :: [(TableName, DataFrame)] -> (TableArray, [DataFrame])
switchListToTuple' [] = ([], [])
switchListToTuple' tuple = (getTNs tuple, getDFs tuple)

getTNs :: [(TableName, DataFrame)] -> TableArray
getTNs [] = []
getTNs ((tn, df) : xs) = getTN (tn, df) ++ getTNs xs

getTN :: (TableName, DataFrame) -> TableArray
getTN (tn, _) = [tn]

getDFs :: [(TableName, DataFrame)] -> [DataFrame]
getDFs [] = []
getDFs ((tn, df) : xs) = getDF (tn, df) ++ getDFs xs

getDF :: (TableName, DataFrame) -> [DataFrame]
getDF (_, df) = [df]

---------------------------------------some update stuff starts--------------------------


filterConditionVol2 :: [Column] -> [Row] -> Condition -> [Condition] -> Either ErrorMessage [Row]
filterConditionVol2 _ [] _ _ = Right []
filterConditionVol2 columns (x:xs) condition selectUpdate =
  if conditionResult columns x condition
    then case changeByRequest columns x selectUpdate of
           Right newRow -> (newRow :) <$> filterConditionVol2 columns xs condition selectUpdate
           Left err -> Left err
    else (x :) <$> filterConditionVol2 columns xs condition selectUpdate

changeByRequest :: [Column] -> Row -> [Condition] -> Either ErrorMessage Row
changeByRequest df row [] = Right row
changeByRequest columns row (condition:conditions) =
  case forOneCondition columns row condition of
    Right newRow -> changeByRequest columns newRow conditions
    Left err -> Left err


forOneCondition :: [Column] -> Row -> Condition -> Either ErrorMessage Row
forOneCondition columns row condition = do
  let conditions = whereConditionColumnName condition
  let values = whereConditionValues condition
  (if hasTwoColumnNames conditions
    then changeByRequestTwoColumns columns row conditions values
    else if hasTwoValues values
      then Left "The conditions are not valid"
        else changeByRequestForOne columns row conditions values)

changeByRequestTwoColumns :: [Column] -> [DF.Value] -> [ColumnName] -> [DF.Value] -> Either ErrorMessage [DF.Value]
changeByRequestTwoColumns columns row setColumn setValue = do
  let index1 = findColumnIndex (head setColumn) columns
  let index2 = findColumnIndex (last setColumn) columns
  let foundElem = row !! index2
  let newRow = replaceNth index1 foundElem row
  case newRow of
    _ -> Right newRow
    [] -> Left "Sorry"

changeByRequestForOne :: [Column] -> [DF.Value] -> [ColumnName] -> [DF.Value] -> Either ErrorMessage [DF.Value]
changeByRequestForOne columns row setColumn setValue = do
  let index = findColumnIndex (head setColumn) columns
  let newRow = replaceNth index (head setValue) row
  case newRow of
    _ -> Right newRow
    [] -> Left "Sorry"

replaceNth :: Int -> DF.Value -> [DF.Value] -> [DF.Value]
replaceNth _ _ [] = []
replaceNth n newVal (x:xs)
  | n == 0 = newVal:xs
  | otherwise = x:replaceNth (n-1) newVal xs

whereConditionValues :: Condition -> [DF.Value]
whereConditionValues (Condition op1 _ op2) =
  case op1 of
    ConstantOperand name1 -> case op2 of
      ConstantOperand name2 -> [name1] ++ [name2]
      _ -> [name1]
    ColumnOperand _ -> case op2 of
      ConstantOperand name -> [name]
      ColumnOperand _ -> []

hasTwoValues :: [DF.Value] -> Bool
hasTwoValues values = length values == 2

hasTwoColumnNames :: [ColumnName] -> Bool
hasTwoColumnNames columnNames = length columnNames == 2

filterSelectVol2 :: DataFrame -> [Condition] -> [Condition] -> Either ErrorMessage DataFrame
filterSelectVol2 df [] _ = Right df
filterSelectVol2 df@(DataFrame colsOg rowsOg) selectUpdate wh@(x:xs) = do
  let columns = whereConditionColumnList selectUpdate
  let columnsAlso = whereConditionColumnList wh
  case doColumnsExist columns df && doColumnsExist columnsAlso df of
    True -> do
      filteredRows <- filterConditionVol2 colsOg rowsOg x selectUpdate
      filterSelectVol2 (DataFrame colsOg filteredRows) xs selectUpdate
    False -> Left "The provided columns do not exist in this table"


---------------------------------------some update stuff ends----------------------------

---------------------------------------some insert stuff starts----------------------------

getColumnNamesVol2 :: SelectedColumns -> [ColumnName]
getColumnNamesVol2 (ColumnsSelected columns) = columns

insertColumnsProvided :: DeserializedContent -> SelectedColumns -> InsertedValues -> Execution (Either ErrorMessage DataFrame)
insertColumnsProvided deserializedContent justColumns values =
  case doColumnsExist (getColumnNamesVol2 justColumns) (snd deserializedContent) of
    True ->
      if insertCheckCounts justColumns values then (case insertColumnsProvidedDeserializedContent deserializedContent justColumns values of
        Left errMsg -> return $ Left errMsg
        Right updatedDataFrame -> do
          saveFile (fst deserializedContent, updatedDataFrame)
          return (Right updatedDataFrame)) else return $ Left "Behold! I, your sovereign, detect errors in thy counts of values and columns. Attend swiftly, rectify this ledger amiss, for accuracy befits our royal domain."
    False -> return $ Left "The provided columns do not exist in the table"

insertColumnsProvidedDeserializedContent :: DeserializedContent -> SelectedColumns -> InsertedValues -> Either ErrorMessage DataFrame
insertColumnsProvidedDeserializedContent (tableName, DataFrame columns rows) changedColumns newValues =
    Right $ DataFrame columns (rows ++ [insertColumnsProvidedRecurssion columns changedColumns newValues])

insertColumnsProvidedRecurssion :: [Column] -> SelectedColumns -> InsertedValues -> [DF.Value]
insertColumnsProvidedRecurssion [] _ _ = []
insertColumnsProvidedRecurssion (Column colName _ : restColumns) (ColumnsSelected changedColumns) newValues =
    case elemIndex colName changedColumns of
        Just index -> newValues !! index : restValues
        Nothing -> NullValue : restValues
  where
    restValues = insertColumnsProvidedRecurssion restColumns (ColumnsSelected changedColumns) newValues

insertToAll :: DeserializedContent -> [DF.Value] -> Execution (Either ErrorMessage DataFrame)
insertToAll (tableName, loadedDataFrame) values = do
    let checkCount = insertToAllCheckCounts loadedDataFrame values
    either
        (return . Left)
        (\newValues -> do
            let updatedDataFrame = insertToAllDataFrame loadedDataFrame newValues
            saveFile (tableName, updatedDataFrame)
            return $ Right updatedDataFrame)
        checkCount

insertCheckCounts :: SelectedColumns -> InsertedValues -> Bool
insertCheckCounts (ColumnsSelected colNames) values = length values == length colNames

insertToAllCheckCounts :: DataFrame -> [DF.Value] -> Either ErrorMessage [DF.Value]
insertToAllCheckCounts (DataFrame columns _) values
    | length values == length columns = Right values
    | otherwise = Left "The columns count is not the same as the provided values count"

insertToAllDataFrame :: DataFrame -> [DF.Value] -> DataFrame
insertToAllDataFrame (DataFrame columns rows) newValues =
    DataFrame columns (rows ++ [newValues])


---------------------------------------some insert stuff ends----------------------------

---------------------------------------some delete stuff starts--------------------------

deleteExecution :: DataFrame -> Maybe [Condition] -> Either ErrorMessage DataFrame
deleteExecution contains Nothing = Right $ leaveOnlyColumnNames contains
deleteExecution contains (Just conditions) =
  case isFaultyConditions conditions of
    False -> case doColumnsExist (whereConditionColumnList conditions) contains of
      True -> case areRowsEmpty (filterSelect contains conditions) of
        False -> Right (uncurry createSelectDataFrame (getColumnsRows (columnsToList (contains)) (reversedFilterSelect (contains) conditions)))
        True -> Right $ contains
      False -> Left "The specified column doesn't exist"
    True -> Left "Conditions are faulty"

leaveOnlyColumnNames :: DataFrame -> DataFrame
leaveOnlyColumnNames (DataFrame columns _) = DataFrame columns []

reversedFilterSelect :: DataFrame -> [Condition] -> DataFrame
reversedFilterSelect df [] = df
reversedFilterSelect (DataFrame colsOg rowsOg) conditions =
  reversedFilterSelect (DataFrame colsOg (reversedFilterCondition colsOg rowsOg conditions)) []

reversedFilterCondition :: [Column] -> [Row] -> [Condition] -> [Row]
reversedFilterCondition _ rows [] = rows
reversedFilterCondition columns rows conditions =
  filter (\x -> not $ all (\condition -> conditionResult columns x condition) conditions) rows


---------------------------------------some delete stuff ends----------------------------

createNowDataFrame :: [Row] -> DataFrame
createNowDataFrame time = DataFrame [Column "Now" StringType] time

uTCToString :: UTCTime -> [Row]
uTCToString utcTime = [[StringValue (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" (addUTCTime (120*60) utcTime))]]


-------------------------------some JSON stuff------------------------------------

toJSONtable :: (TableName, (DataFrame)) -> String
toJSONtable table = "{\"Table\":"++"" ++ show (fst (table)) ++ ""++",\"Columns\":[" ++ toJSONColumns (toColumnList (snd (table))) ++ "],\"Rows\":[" ++ toJSONRows (toRowList (snd (table))) ++"]}"

toJSONColumn :: Column -> String
toJSONColumn column = "{\"Name\":"++"" ++ show (getColumnName (column)) ++ ",\"ColumnType\":"++"\"" ++ show (getType (column)) ++ "\""++"}"

toJSONRowValue :: DF.Value -> String
toJSONRowValue value = "{\"Value\":"++"\"" ++ fixedValueString (show (value)) ++ "\""++"}"

fixedValueString :: String -> String
fixedValueString [] = []
fixedValueString (x:xs)
  |x /= '"' = [x] ++ fixedValueString xs
  |otherwise = fixedValueString xs
---------------------------------------some get stuff----------------------------

toColumnList :: DataFrame -> [Column]
toColumnList (DataFrame col row)  = col

toRowList :: DataFrame -> [Row]
toRowList (DataFrame co row) = row

----------------------------------recursive stuff-------------------------------

toJSONColumns :: [Column] -> String
toJSONColumns (x:xs)
  |xs /= [] = toJSONColumn x ++ "," ++ toJSONColumns xs
  |otherwise = toJSONColumn x

toJSONRow :: Row -> String
--toJSONRow [] = []
toJSONRow (x:xs)
  |xs /= [] = toJSONRowValue x ++ "," ++ toJSONRow xs
  |otherwise = toJSONRowValue x

toJSONRows :: [Row] -> String
toJSONRows [] = []
toJSONRows (x:xs)
  |xs /= [] = "{\"Row\":[" ++ toJSONRow x ++ "]}," ++ toJSONRows xs
  |otherwise = "{\"Row\":[" ++ toJSONRow x ++ "]}"


--------------------------------Files--------------------------------------------

toFilePath :: TableName -> FilePath
toFilePath tableName = "db/" ++ show (tableName) ++ ".json" --".txt"

serializedContent :: (TableName, DataFrame) -> Either ErrorMessage FileContent
serializedContent table = case checkRows table of
  True -> return (serializedTable table)
  False -> do
    _ <- Lib1.validateDataFrame (snd table)
    return (serializedTable table)

checkRows :: (TableName, DataFrame) -> Bool
checkRows (_, DataFrame _ rows)
  | rows == [] =  True
  | otherwise = False

serializedTable :: (TableName, DataFrame) -> FileContent
serializedTable table = (toJSONtable (table))

---------------------------ParseStatement-------------------------------------

parseStatement2 :: String -> Either ErrorMessage ParsedStatement2
parseStatement2 query = case runParser p query of
    Left err1 -> Left err1
    Right (query, rest) -> case query of
        Lib3.Select _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Lib3.ShowTable _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Lib3.ShowTables -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Lib3.SelectAll _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Insert _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Update _ _ _-> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Delete _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        SelectNow -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        DropTable _  -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        CreateTable _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
    where
        p :: Parser ParsedStatement2
        p = showTableParser
               <|> showTablesParser
               <|> selectStatementParser
               <|> selectAllParser
               <|> insertParser
               <|> updateParser
               <|> deleteParser
               <|> selectNowParser
               <|> dropTableParser 

--------------------ParseStatement ends-----------------------------------------
-----------------Executes for show and select statements------------------------

executeShowTable :: DataFrame -> TableName -> DataFrame
executeShowTable df table = createColumnsDataFrame (columnsToList df) table

executeShowTables :: TableArray -> DataFrame
executeShowTables tables = createTablesDataFrame tables

executeSelectAll :: TableArray -> [DataFrame] -> Maybe WhereSelect -> Either ErrorMessage DataFrame
executeSelectAll tables selectedDfs whereSelect = case areTablesValid selectedDfs of
    True -> case whereSelect of
      Just conditions -> case isFaultyConditions conditions of
        False -> case doColumnsExistDFs (whereConditionColumnList conditions) selectedDfs of
          True -> case doColumnsExistProvidedDfs tables selectedDfs (whereConditionColumnList2 conditions) of
            True -> case checkForMatchingColumns (getAllColumnsCartesianDF (createCartesianDataFrame selectedDfs tables)) (whereConditionColumnList conditions) of
              True -> case areRowsEmpty (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) of
                False -> Right $ deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions
                True -> Left "There are no results with the provided conditions or the condition is faulty"
              False -> Left "Some of provided column names are ambiguous"
            False -> Left "Some of provided columns do not exist in provided tables or expected table where not provided after 'from'"
          False -> Left "Some of provided columns do not exist in provided tables"
        True -> Left "Conditions are faulty"
      Nothing -> Right $ cartesianDataFrame selectedDfs
    False -> Left "Some of provided tables are not valid"

executeSelectWithAllSauces :: SpecialSelect2 -> TableArray -> [DataFrame] -> Maybe WhereSelect -> UTCTime -> Either ErrorMessage DataFrame
executeSelectWithAllSauces specialSelect tables selectedDfs whereSelect time = case specialSelect of
  (SelectColumn2 specialColumns nowFunction) -> case doColumnsExistDFs specialColumns selectedDfs of
    True -> case checkForMatchingColumns (getAllColumnsCartesianDF (createCartesianDataFrame selectedDfs tables)) specialColumns of
      True -> case areTablesValid selectedDfs of
        True -> case whereSelect of
          Just conditions -> case isFaultyConditions conditions of
            False -> case doColumnsExistDFs (whereConditionColumnList conditions) selectedDfs of
              True -> case doColumnsExistProvidedDfs tables selectedDfs (whereConditionColumnList2 conditions) of
                True -> case checkForMatchingColumns (getAllColumnsCartesianDF (createCartesianDataFrame selectedDfs tables)) (whereConditionColumnList conditions) of
                  True -> case areRowsEmpty (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) of
                    False -> case nowFunction of
                      Just _ -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([uncurry createSelectDataFrame 
                                  $ getColumnsRows specialColumns (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
                      Nothing -> Right $ uncurry createSelectDataFrame 
                                  $ getColumnsRows specialColumns (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)
                    True -> Left "There are no results with the provided conditions or the condition is faulty"
                  False -> Left "Some of provided column names are ambiguous"
                False -> Left "Some of provided columns do not exist in provided tables or expected table where not provided after 'from'"
              False -> Left "Some of provided columns do not exist in provided tables"
            True -> Left "Conditions are faulty"
          Nothing -> case nowFunction of
            Just _ -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([uncurry createSelectDataFrame $ getColumnsRows specialColumns (deCartesianDataFrame $ createCartesianDataFrame selectedDfs tables)] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
            Nothing -> Right $ uncurry createSelectDataFrame $ getColumnsRows specialColumns (deCartesianDataFrame $ createCartesianDataFrame selectedDfs tables)
        False -> Left "Some of provided tables are not valid"
      False -> Left "Some of provided column names are ambiguous"
    False -> Left "Some of provided columns do not exist in provided tables"

  (SelectedColumnsTables specialColumns nowFunction) -> case doColumnsExistProvidedDfs tables selectedDfs specialColumns of
    True -> case areTablesValid selectedDfs of
      True -> case whereSelect of
        Just conditions -> case isFaultyConditions conditions of
          False -> case doColumnsExistDFs (whereConditionColumnList conditions) selectedDfs of
            True -> case doColumnsExistProvidedDfs tables selectedDfs (whereConditionColumnList2 conditions) of
              True -> case areRowsEmpty (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) of
                False -> case nowFunction of
                  Just _ -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([uncurry createSelectDataFrame 
                              $ (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions), snd $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions))] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
                  Nothing -> Right $ uncurry createSelectDataFrame 
                              $ (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions), snd $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions))
                True -> Left "There are no results with the provided conditions or the condition is faulty"
              False -> Left "Some of provided column names are ambiguous"
            False -> Left "Some of provided columns do not exist in provided tables"
          True -> Left "Conditions are faulty"
        Nothing -> case nowFunction of
          Just _ -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([uncurry createSelectDataFrame 
                              (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables), snd $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables))] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
          Nothing -> Right $ uncurry createSelectDataFrame 
                              (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables), snd $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables))
      False -> Left "Some of provided table(s) are not valid"
    False -> Left "Some of provided columns do not exist in provided tables"

  (SelectAggregate2 aggregatesList nowFunction) -> case areTablesValid selectedDfs of
    True -> case doColumnsExistDFs (getColumnsFromAggregates aggregatesList) selectedDfs of
      True -> case checkForMatchingColumns (getAllColumnsCartesianDF (createCartesianDataFrame selectedDfs tables)) (getColumnsFromAggregates aggregatesList)of
        True -> case doColumnsExistProvidedDfs tables selectedDfs (getColumnsTablesAggregatesList $ getColumnsTablesAggregates aggregatesList) of
          True -> case whereSelect of
            Just conditions -> case isFaultyConditions conditions of
              False -> case doColumnsExistDFs (whereConditionColumnList conditions) selectedDfs of
                True -> case doColumnsExistProvidedDfs tables selectedDfs (whereConditionColumnList2 conditions) of
                  True -> case areRowsEmpty (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) of
                    False -> case nowFunction of
                      Just _ -> case processSelect2 (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                        Right (cols, rows) -> case null cols of
                          True -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> Left "There are no results"
                              False -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createSelectDataFrame cols2 [rows2]] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
                            Left err -> Left err
                          False -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createSelectDataFrame cols [rows]] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
                              False -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createSelectDataFrame (cols ++ cols2) [rows ++ rows2]] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"]) 
                            Left err -> Left err
                        Left err -> Left err
                      Nothing -> case processSelect2 (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                        Right (cols, rows) -> case null cols of
                          True -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> Left "There are no results"
                              False -> Right $ createSelectDataFrame cols2 [rows2]
                            Left err -> Left err
                          False -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> Right $ createSelectDataFrame cols [rows]
                              False -> Right $ createSelectDataFrame (cols ++ cols2) [rows ++ rows2]
                            Left err -> Left err
                        Left err -> Left err
                    True -> Left "There are no results with the provided conditions or the condition is faulty"
                  False -> Left "Some of provided column names are ambiguous"
                False -> Left "Some of provided columns do not exist in provided tables"
              True -> Left "Conditions are faulty"
            Nothing -> case nowFunction of
              Just _ -> case processSelect2 (createCartesianDataFrame selectedDfs tables) aggregatesList of
                Right (cols, rows) -> case null cols of
                  True -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> Left "There are no results"
                      False -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createSelectDataFrame cols2 [rows2]] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"]) 
                    Left err -> Left err
                  False -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createSelectDataFrame cols [rows]] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"]) 
                      False -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createSelectDataFrame (cols ++ cols2) [rows ++ rows2]] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"]) 
                    Left err -> Left err
                Left err -> Left err
              Nothing -> case processSelect2 (createCartesianDataFrame selectedDfs tables) aggregatesList of
                Right (cols, rows) -> case null cols of
                  True -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> Left "There are no results"
                      False -> Right $ createSelectDataFrame cols2 [rows2]
                    Left err -> Left err
                  False -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> Right $ createSelectDataFrame cols [rows]
                      False -> Right $ createSelectDataFrame (cols ++ cols2) [rows ++ rows2]
                    Left err -> Left err
                Left err -> Left err
          False -> Left "Some of provided columns in aggregate functions do not exist in provided tables"
        False -> Left "Some of provided columns in aggregate functions are ambiguous" 
      False -> Left "Some of provided columns in aggregate functions do not exist in provided tables" 
    False -> Left "Some of provided table(s) are not valid"

------------------------------utilities for select execution-------------------

areTablesValid :: [DataFrame] -> Bool
areTablesValid [] = True
areTablesValid (x:xs)
  | Lib2.validateDataFrame x = areTablesValid xs 
  | otherwise = False
-----------------------------------su columns:----------------------------------
processSelect2 :: CartesianDataFrame -> [Aggregate2] -> Either ErrorMessage ([Column],Row)
processSelect2 cdf aggregateList =
  case processSelectAggregates (deCartesianDataFrame cdf) (getColumnsAggregates aggregateList) of 
    Right tuples -> Right (fst (switchListToTuple tuples), head $ snd (switchListToTuple tuples))
    Left err -> Left err
-----------------------su columns ir ju tables:---------------------------------
processSelect2' :: CartesianDataFrame -> [Aggregate2] -> Either ErrorMessage ([Column],Row)
processSelect2' cdf aggregateList =
  case processSelectAggregates' cdf (getColumnsTablesAggregates aggregateList) of 
    Right tuple -> Right (fst (switchListToTuple tuple), head $ snd (switchListToTuple tuple))
    Left err -> Left err

processSelectAggregates' :: CartesianDataFrame -> [(AggregateFunction, (TableName, ColumnName))] -> Either ErrorMessage [(Column, Row)]
processSelectAggregates' _ [] = Right []
processSelectAggregates' (CartesianDataFrame cc rows) ((func,(tableName, columnName)):xs) =
  case func of
    Max -> do
      let maxResult = findMax $ rowListToRow (getCartesianRows cc rows [(tableName,columnName)])
      restResult <- processSelectAggregates' (CartesianDataFrame cc rows) xs
      let newColumn = Column ("Max "++ tableName ++ "." ++ columnName) (head $ getColumnTablesType [columnName] tableName cc)
      return $ (newColumn, fromRight [] maxResult) : restResult
    Sum -> do
      case (head $ getColumnTablesType [columnName] tableName cc) == IntegerType of
        True -> do
          let sumResult = findSum $ rowListToRow (getCartesianRows cc rows [(tableName,columnName)])
          restResult <- processSelectAggregates' (CartesianDataFrame cc rows) xs
          let newColumn = Column ("Sum "++ tableName ++ "." ++columnName) (head $ getColumnTablesType [columnName] tableName cc)
          return $ (newColumn, fromRight [] sumResult) : restResult
        False -> Left "Selected column should have integers"

getColumnTablesType :: [ColumnName] -> TableName -> [CartesianColumn] -> [ColumnType]
getColumnTablesType [] _ _ = []
getColumnTablesType (x:xs) table col = columnTableType col 0 (findColumnTableIndex x table col) : getColumnTablesType xs table col

columnTableType :: [CartesianColumn] -> Int -> Int -> ColumnType
columnTableType (x:xs) i colIndex
  | i == colIndex = getColumnTableType x
  | otherwise = columnTableType xs (i+1) colIndex

getColumnTableType :: CartesianColumn -> ColumnType
getColumnTableType (CartesianColumn (_, Column _ colType)) = colType

--------------------------------------------------------------------------------
getConcatColumnsRows :: ([Column], [Row]) -> ([Column], [Row]) -> ([Column], [Row])
getConcatColumnsRows (col1, row1) (col2, row2) = (col1++col2, getConcatRows row1 row2)

getConcatRows :: [Row] -> [Row] -> [Row]
getConcatRows [] [] = []
getConcatRows (x:xs) (y:ys) = [x ++ y] ++ getConcatRows xs ys

--patikrinimui ar ne ambiguous
getColumnsFromAggregates :: [Aggregate2] -> [ColumnName]
getColumnsFromAggregates [] = []
getColumnsFromAggregates (x:xs) =
  case x of
    AggregateColumn (_, columnName) -> columnName : getColumnsFromAggregates xs
    AggregateColumnTable _ -> getColumnsFromAggregates xs

getColumnsAggregates :: [Aggregate2] -> [(AggregateFunction, ColumnName)]
getColumnsAggregates [] = []
getColumnsAggregates (x:xs) =
  case x of
    AggregateColumn a -> a : getColumnsAggregates xs
    AggregateColumnTable _ -> getColumnsAggregates xs

getColumnsTablesAggregates :: [Aggregate2] -> [(AggregateFunction, (TableName, ColumnName))]
getColumnsTablesAggregates [] = []
getColumnsTablesAggregates (x:xs) =
  case x of
    AggregateColumnTable a -> a : getColumnsTablesAggregates xs
    AggregateColumn _ -> getColumnsTablesAggregates xs

getColumnsTablesAggregatesList :: [(AggregateFunction, (TableName, ColumnName))] -> [(TableName, ColumnName)]
getColumnsTablesAggregatesList [] = []
getColumnsTablesAggregatesList ((_,(tn, cn)):xs) = (tn, cn) : getColumnsTablesAggregatesList xs 

------------------Stuff with cartesian products and dataframes-----------------

getColumnsTablesList :: [(TableName, ColumnName)] -> CartesianDataFrame -> ([CartesianColumn], [Row])
getColumnsTablesList colsWithTables (CartesianDataFrame cols rows) = (getCartesianColumns colsWithTables cols, getCartesianRows cols rows colsWithTables)

getCartesianColumns :: [(TableName, ColumnName)] -> [CartesianColumn] -> [CartesianColumn]
getCartesianColumns [] _ = []
getCartesianColumns (x:xs) cc = getCartesianColumn x cc : getCartesianColumns xs cc

getCartesianColumn :: (TableName, ColumnName) -> [CartesianColumn] -> CartesianColumn
getCartesianColumn (tn, cn) (cc@(CartesianColumn (table, Column name _)):xs)
  | tn /= table || cn /= name = getCartesianColumn (tn,cn) xs
  | otherwise = cc

getCartesianRows :: [CartesianColumn] -> [Row] -> [(TableName, ColumnName)] -> [Row]
getCartesianRows _ [] _ = []
getCartesianRows cc (x:xs) colsWithTables = getCartesianRow x cc colsWithTables : getCartesianRows cc xs colsWithTables

getCartesianRow :: [DF.Value] -> [CartesianColumn] -> [(TableName, ColumnName)] -> [DF.Value]
getCartesianRow _ _ [] = []
getCartesianRow row cc ((tn,cn):xs) = getValueFromRow row (findColumnTableIndex cn tn cc) 0 : getCartesianRow row cc xs

cartesianDataFrame :: [DataFrame] -> DataFrame
cartesianDataFrame [] = DataFrame [] []
cartesianDataFrame (df@(DataFrame cols rows):xs) = case getNextDataFrame xs of
  Nothing -> df
  Just nextDf -> cartesianDataFrame $ [DataFrame (cols ++ dcols) (cartesianProduct rows drows)] ++ rest
    where
      DataFrame dcols drows = nextDf
      rest = filter (/= nextDf) xs

cartesianProduct :: [[a]] -> [[a]] -> [[a]]
cartesianProduct xs ys = [x ++ y | x <- xs, y <- ys]

createCartesianDataFrame :: [DataFrame] -> TableArray -> CartesianDataFrame
createCartesianDataFrame df tables = getCartesianDataFrame $ createCartesianDataFrames df tables

createCartesianDataFrames :: [DataFrame] -> TableArray -> [CartesianDataFrame]
createCartesianDataFrames ((DataFrame cols rows):xs) (y:ys) = case getNextDataFrame xs of
  Nothing -> [CartesianDataFrame (createCartesianColumns cols y) rows]
  Just (DataFrame dcols drows) -> [CartesianDataFrame (createCartesianColumns cols y) rows] ++ createCartesianDataFrames xs ys

createCartesianColumns :: [Column] -> TableName -> [CartesianColumn]
createCartesianColumns (x:xs) table = case getNextDataFrame xs of
  Nothing -> [CartesianColumn (table, x)]
  Just _ -> [CartesianColumn (table, x)] ++ createCartesianColumns xs table

getCartesianDataFrame :: [CartesianDataFrame] -> CartesianDataFrame
getCartesianDataFrame [] = CartesianDataFrame [] []
getCartesianDataFrame (cdf@(CartesianDataFrame cols rows):xs) =
    case getNextDataFrame xs of
      Nothing -> cdf
      Just nextCdf ->
          getCartesianDataFrame $ [CartesianDataFrame (cols ++ dcols) (cartesianProduct rows drows)] ++ rest
            where
              CartesianDataFrame dcols drows = nextCdf
              rest = filter (/= nextCdf) xs

deCartesianDataFrame :: CartesianDataFrame -> DataFrame
deCartesianDataFrame (CartesianDataFrame cols rows) = DataFrame (deCartesianColumns cols) rows

deCartesianColumns :: [CartesianColumn] -> [Column]
deCartesianColumns [] = []
deCartesianColumns ((CartesianColumn (_, col)):xs) = [col] ++ deCartesianColumns xs

getNextDataFrame :: [a] -> Maybe a
getNextDataFrame [] = Nothing
getNextDataFrame (y:_) = Just y

--------------------------------------------------------------------------------

doColumnsExistDFs :: [ColumnName] -> [DataFrame] -> Bool
doColumnsExistDFs _ [] = False
doColumnsExistDFs colNames dataFrames = all (\colName -> any (\df -> doColumnsExist [colName] df) dataFrames) colNames

whereConditionColumnList2 :: [Condition] -> [(TableName, ColumnName)]
whereConditionColumnList2 [] = []
whereConditionColumnList2 (x:xs) = whereConditionColumnName2 x ++ whereConditionColumnList2 xs

whereConditionColumnName2 :: Condition -> [(TableName, ColumnName)]
whereConditionColumnName2 (Condition op1 _ op2) =
  case op1 of
    ColumnTableOperand name1 -> case op2 of
      ColumnTableOperand name2 -> [name1] ++ [name2]
      _ -> [name1]
    ConstantOperand _ -> case op2 of
      ColumnTableOperand name -> [name]
      ConstantOperand _ -> []
    ColumnOperand _ -> []

doColumnsExistProvidedDfs :: TableArray -> [DataFrame] -> [(TableName, ColumnName)] -> Bool
doColumnsExistProvidedDfs _ _ [] = True
doColumnsExistProvidedDfs ta dfs ((table, column):xs)
  | checkIfConditionsMatchesWithData ta dfs (table, column) = doColumnsExistProvidedDfs ta dfs xs
  | otherwise = False

checkIfConditionsMatchesWithData :: TableArray -> [DataFrame] -> (TableName, ColumnName) -> Bool
checkIfConditionsMatchesWithData [] _ _ = False
checkIfConditionsMatchesWithData _ [] _ = False
checkIfConditionsMatchesWithData (table:tables) (df:dataFrames) (targetTable, targetColumn)
  | table == targetTable && doColumnsExist [targetColumn] df = True
  | otherwise = checkIfConditionsMatchesWithData tables dataFrames (targetTable, targetColumn)

checkForMatchingColumns :: [ColumnName] -> [ColumnName] -> Bool
checkForMatchingColumns _ [] = True
checkForMatchingColumns columns (x:xs)
  | checkForMoreThanOne columns x 0 < 2 = checkForMatchingColumns columns xs
  | otherwise = False

checkForMoreThanOne :: [ColumnName] -> ColumnName -> Int -> Int
checkForMoreThanOne [] _ i = i
checkForMoreThanOne (x:xs) column i =
  if x == column
    then checkForMoreThanOne xs column (i + 1)
    else checkForMoreThanOne xs column i

getAllColumnsCartesianDF :: CartesianDataFrame -> [ColumnName]
getAllColumnsCartesianDF (CartesianDataFrame cols _) = getAllColumnNamesCartesianDF $ getColumnListFromCartesianDF cols

getAllColumnNamesCartesianDF :: [Column] -> [ColumnName]
getAllColumnNamesCartesianDF [] = []
getAllColumnNamesCartesianDF ((Column name _):xs) = [name] ++ getAllColumnNamesCartesianDF xs

------------------------------filter selectAll----------------------------------

filterSelectAll :: CartesianDataFrame -> [Condition] -> CartesianDataFrame
filterSelectAll df [] = df
filterSelectAll (CartesianDataFrame colsOg rowsOg) (x:xs) = filterSelectAll (CartesianDataFrame colsOg $ filterConditionAll colsOg rowsOg x) xs

filterConditionAll :: [CartesianColumn] -> [Row] -> Condition -> [Row]
filterConditionAll _ [] _ = []
filterConditionAll cartesianColumns (x:xs) condition =
  if conditionResultAll cartesianColumns x condition
    then [x] ++ filterConditionAll cartesianColumns xs condition
    else filterConditionAll cartesianColumns xs condition

conditionResultAll :: [CartesianColumn] -> Row -> Condition -> Bool
conditionResultAll cartesianColumns row (Condition op1 operator op2) =
  let v1 = getFilteredValueAll op1 cartesianColumns row
      v2 = getFilteredValueAll op2 cartesianColumns row
  in
    case operator of
    IsEqualTo -> v1 == v2
    IsNotEqual -> v1 /= v2
    IsLessThan -> v1 < v2
    IsGreaterThan -> v1 > v2
    IsLessOrEqual -> v1 <= v2
    IsGreaterOrEqual -> v1 >= v2

getFilteredValueAll :: Operand -> [CartesianColumn] -> Row -> DF.Value
getFilteredValueAll (ConstantOperand value) _ _ = value
getFilteredValueAll (ColumnOperand columnName) cartesianColumns row = getValueFromRow row (findColumnIndex columnName $ getColumnListFromCartesianDF cartesianColumns) 0
getFilteredValueAll (ColumnTableOperand (table, columnName)) cartesianColumns row = getValueFromRow row (findColumnTableIndex columnName table cartesianColumns) 0

findColumnTableIndex :: ColumnName -> TableName -> [CartesianColumn] -> Int
findColumnTableIndex columnName tableName columns = columnTableIndex columnName tableName columns 0

columnTableIndex :: ColumnName -> TableName -> [CartesianColumn] -> Int -> Int
columnTableIndex _ _ [] _ = -1
columnTableIndex columnName tableName ((CartesianColumn (table, (Column name _))):xs) index
    | columnName /= name || tableName /= table = (columnTableIndex columnName tableName xs (index + 1))
    | otherwise = index

getColumnListFromCartesianDF :: [CartesianColumn] -> [Column]
getColumnListFromCartesianDF [] = []
getColumnListFromCartesianDF ((CartesianColumn (table, column)):xs) = [column] ++ getColumnListFromCartesianDF xs

----------------------------end of select execute utillities--------------------
--------------------------------------------------------------------------------
------------------------------THE parsers---------------------------------------


insertParser :: Parser ParsedStatement2
insertParser = do
    _ <- queryStatementParser "insert"
    _ <- whitespaceParser
    _ <- queryStatementParser "into"
    _ <- whitespaceParser
    tableName <- columnNameParser
    _ <- optional whitespaceParser
    selectUpdate <- optional selectDataParsers
    _ <- optional whitespaceParser
    _ <- queryStatementParser "values"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    values <- insertedValuesParser
    _ <- queryStatementParser ")"
    pure $ Insert tableName selectUpdate values

selectDataParsers :: Parser SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      _ <- queryStatementParser "("
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- queryStatementParser ")"
      return $ ColumnsSelected columnNames

insertedValuesParser :: Parser InsertedValues
insertedValuesParser = do
  values <- seperate valueParser (queryStatementParser "," >> optional whitespaceParser)
  return values

valueParser :: Parser DF.Value
valueParser = parseNullValue <|> parseBoolValue <|> parseStringValue <|> parseNumericValue

parseNullValue :: Parser DF.Value
parseNullValue = queryStatementParser "null" >> return NullValue

parseBoolValue :: Parser DF.Value
parseBoolValue =
  (queryStatementParser "true" >> return (BoolValue True))
  <|> (queryStatementParser "false" >> return (BoolValue False))

parseStringValue :: Parser DF.Value
parseStringValue = do
  strValue <- parseStringWithQuotes
  return (StringValue strValue)

parseStringWithQuotes :: Parser String
parseStringWithQuotes = do
  _ <- queryStatementParser "'"
  str <- some (parseSatisfy (/= '\''))
  _ <- queryStatementParser "'"
  return str

parseNumericValue :: Parser DF.Value
parseNumericValue = IntegerValue <$> parseInt

parseInt :: Parser Integer
parseInt = do
  digits <- some (parseSatisfy isDigit)
  return (read digits)

deleteParser :: Parser ParsedStatement2
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser ParsedStatement2
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
    selectUpdated <- setParser
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParser :: Parser WhereSelect
setParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "set"
  _ <- whitespaceParser
  seperate whereConditionParser (optional whitespaceParser >> char ',' >> optional whitespaceParser)

----Lib2 stuff----
selectAllParser :: Parser ParsedStatement2
selectAllParser = do
  _ <- queryStatementParser "select"
  _ <- whitespaceParser
  _ <- queryStatementParser "*"
  _ <- whitespaceParser
  _ <- queryStatementParser "from"
  _ <- whitespaceParser
  tableArray <- selectTablesParser
  selectWhere <- optional whereParser
  _ <- optional whitespaceParser
  pure $ Lib3.SelectAll tableArray selectWhere

selectNowParser :: Parser ParsedStatement2
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow

selectStatementParser :: Parser ParsedStatement2
selectStatementParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    specialSelect <- selectDataParser
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableArray <- selectTablesParser
    selectWhere <- optional whereParser
    _ <- optional whitespaceParser
    pure $ Lib3.Select specialSelect tableArray selectWhere

selectDataParser :: Parser SpecialSelect2
selectDataParser = tryParseAggregate <|>  tryParseColumn <|> tryParseColumnTable
  where
    tryParseAggregate = do
      nowf <- optional nowParser
      aggregateList <- seperate aggregateParser' (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectAggregate2 aggregateList nowf
    tryParseColumn = do
      nowf <- optional nowParser
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectColumn2 columnNames nowf
    tryParseColumnTable = do
      nowf <- optional nowParser
      columnsWithTables <- seperate columnNameTableParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- trashParser
      return $ SelectedColumnsTables columnsWithTables nowf

aggregateParser' :: Parser Aggregate2
aggregateParser' = tryColumnTables <|> tryColumns
  where 
    tryColumns = do
      func <- aggregateFunctionParser
      _ <- optional whitespaceParser
      _ <- char '('
      _ <- optional whitespaceParser
      columnName <- columnNameParser''
      _ <- optional whitespaceParser
      _ <- char ')'
      return $ AggregateColumn (func, columnName)
    tryColumnTables = do
      func <- aggregateFunctionParser
      _ <- optional whitespaceParser
      _ <- char '('
      _ <- optional whitespaceParser
      columnName <- columnNameTableParser
      _ <- optional whitespaceParser
      _ <- char ')'
      return $ AggregateColumnTable (func, columnName)

nowParser :: Parser NowFunction
nowParser = do
  _ <- optional whitespaceParser
  _ <- queryStatementParser "now"
  _ <- optional whitespaceParser
  _ <- queryStatementParser "("
  _ <- optional whitespaceParser
  _ <- queryStatementParser ")"
  _ <- optional whitespaceParser
  _ <- queryStatementParser ","
  _ <- optional whitespaceParser
  pure Now

whereParser :: Parser WhereSelect
whereParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "where"
  _ <- whitespaceParser
  some whereAndExist
  where
    whereAndExist :: Parser Condition
    whereAndExist = do
      condition <- whereConditionParser
      _ <- optional (whitespaceParser >> andParser)
      pure condition

andParser :: Parser And
andParser = queryStatementParser "and" >> pure And

whereConditionParser :: Parser Condition
whereConditionParser = do
  _ <- optional whitespaceParser
  operand1 <- operandParser
  _ <- optional whitespaceParser
  operator <- operatorParser
  _ <- optional whitespaceParser
  operand2 <- operandParser
  return $ Condition operand1 operator operand2

operandParser :: Parser Operand
operandParser = (ConstantOperand <$> constantParser)
               <|> (ColumnTableOperand <$> columnNameTableParser)
               <|> (ColumnOperand <$> columnNameParser)

columnNameTableParser :: Parser (TableName, ColumnName)
columnNameTableParser = do
  table <- columnNameParser
  _ <- char '.'
  column <- columnNameParser
  return (table, column)

-------------------------------------------------------------------------------

selectTablesParser :: Parser TableArray
selectTablesParser = tryParseTable
  where
    tryParseTable = do
      tableNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      return $ tableNames

showTablesParser :: Parser ParsedStatement2
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure Lib3.ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser ParsedStatement2
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    pure $ Lib3.ShowTable table


dropTableParser :: Parser ParsedStatement2
dropTableParser = do
    _ <- queryStatementParser "drop"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser 
    pure $ DropTable table

createTableParser :: Parser ParsedStatement2
createTableParser = do
    _ <- queryStatementParser "create"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- whitespaceParser
    _ <- char '('
    _ <- optional whitespaceParser
    columnsAndTypes <- columnListParser
    _ <- optional whitespaceParser
    _ <- char ')'
    _ <- optional whitespaceParser
    pure $ CreateTable table columnsAndTypes

columnListParser :: Parser [Column]
columnListParser = seperate columnAndTypeParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)

columnAndTypeParser :: Parser Column
columnAndTypeParser = do
    columnName <- columnNameParser
    _ <- whitespaceParser
    columnType <- columnTypeParser 
    pure (Column columnName columnType)

columnTypeParser :: Parser ColumnType
columnTypeParser = do
    typeName <- columnNameParser
    case typeName of
        "int"    -> pure IntegerType
        "varchar" -> pure StringType
        "bool"   -> pure BoolType
        _        -> pure BoolType