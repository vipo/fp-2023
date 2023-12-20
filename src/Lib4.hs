{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use if" #-}

module Lib4
  (
    ParsedStatement3(..), 
    parseStatement,
    SqlStatement(..), 
    fromStatement,
    toTable,
    toDataframe,
    SqlException(..),
    SqlTableFromYAML(..),
    fromException,
    toStatement,
    ValueFromYAML,
    RowFromYAML,
    ColumnFromYAML,
    fromTable,
    fromDataFrame,
    Execution,
    ExecutionAlgebra(..),
    serializedContent,
    executeSql,
    SpecialSelect2(..),
    OrderByValue(..),
    AscDesc(..),
    Integer,
    SpecialSelect(..),
    NowFunction,
    SelectedColumns(..),
    toException,
    Aggregate2(..)

  )  
where

import Lib2(dropWhiteSpaces, 
            getOperand, 
            stringToInt, 
            isNumber, 
            areSpacesBetweenWords, 
            splitStatementAtParentheses, 
            getColumnName, 
            getType, 
            findColumnIndex,
            createColumnsDataFrame,
            columnsToList,
            createTablesDataFrame,
            whereConditionColumnList,
            isFaultyConditions,
            areRowsEmpty,
            WhereSelect,
            Condition(..),
            Operator(IsGreaterOrEqual, IsEqualTo, IsNotEqual, IsLessThan,
                    IsGreaterThan, IsLessOrEqual),
            Operand(..),
            And(..),
            AggregateFunction(..),
            getColumnsRows,
            createSelectDataFrame,
            conditionResult,
            whereConditionColumnName,
            doColumnsExist,
            filterSelect,
            validateDataFrame,
            processSelectAggregates,
            switchListToTuple,
            findMax,
            rowListToRow,
            findSum,
            getValueFromRow)
import Lib3
  (CartesianColumn(..), 
  CartesianDataFrame(..), 
  UTCTime)
import Lib1 (validateDataFrame)
import Control.Applicative(Alternative(empty, (<|>)),optional, some, many)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT, state)
import Data.Char (toLower, GeneralCategory (ParagraphSeparator), isSpace, isAlphaNum, isDigit, digitToInt)
import Data.String (IsString, fromString)
import Control.Monad.Trans.Class
import Control.Monad.Trans.State.Strict
import DataFrame as DF
    ( Column(..), ColumnType(..), DataFrame(..), Row, Value(..) )
import Data.Aeson.Key
import Data.Char (isDigit)
import GHC.IO.Handle (NewlineMode(inputNL))
import Data.List (isPrefixOf, nub)
import Data.Yaml
import GHC.Generics
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Data.Either (fromRight)
import Data.Bool (Bool(True))
import Control.Monad.Free (Free (..), liftF)
import Control.Monad
import Data.Time
import Data.List (find, findIndex, elemIndex, nub, elem, intercalate, sortBy)

------------------------------------------------------------------------------
type ColumnName = String
type Error = String

type Parser4 a = EitherT Error (State String) a

data AscDesc = Asc String | Desc String
  deriving (Show, Eq)

data OrderByValue = ColumnTable (TableName, ColumnName) | ColumnName ColumnName | ColumnNumber Integer
  deriving (Show, Eq)

type OrderBy = [(OrderByValue, AscDesc)]

type TableName = String
type FileContent = String
type DeserializedContent = (TableName, DataFrame)
type ErrorMessage = String
type TableArray =  [TableName]

data Trash = Trash String
  deriving (Show, Eq)

data NowFunction = Now
  deriving (Show, Eq)

data Aggregate2 = AggregateColumn (AggregateFunction, ColumnName) | AggregateColumnTable (AggregateFunction, (TableName, ColumnName))
  deriving (Show, Eq)

data SpecialSelect2 = SelectAggregate2 [Aggregate2] (Maybe NowFunction) | SelectColumn2 [ColumnName] (Maybe NowFunction) | SelectedColumnsTables [(TableName, ColumnName)] (Maybe NowFunction)
  deriving (Show, Eq)

type AggregateList = [(AggregateFunction, ColumnName)]

data SpecialSelect = SelectAggregate AggregateList | SelectColumns [ColumnName]
  deriving (Show, Eq)

data SelectedColumns = ColumnsSelected [ColumnName]
  deriving (Show, Eq)

type InsertedValues = [DF.Value]

------------------------------------------------------------------------------

data ParsedStatement3 =
  SelectNow {}
  | DropTable {
    table :: TableName
  }
  | CreateTable {
    table :: TableName,
    newColumns :: [Column]
  }
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
    selectWhere :: Maybe WhereSelect,
    order :: Maybe OrderBy
   }
  |Select {
    selectQuery :: SpecialSelect2,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect,
    order :: Maybe OrderBy
  }
  | ShowTables { }
    deriving (Show, Eq)


------------------for communication starts-------------

data SqlStatement = SqlStatement {
  statement :: String
} deriving (Generic, Show)

data SqlException = SqlException {
  exception :: String
} deriving (Generic)

data SqlTableFromYAML = SqlTableFromYAML {
  columnsYAML :: ColumnsFromYAML,
  rowsYAML :: RowsFromYAML
} deriving (Generic)

data ColumnsFromYAML = ColumnsFromYAML{
  columnListYAML :: [ColumnFromYAML]
  } deriving (Generic)

data ColumnFromYAML = ColumnFromYAML{
  columnNameYAML :: String,
  columnTypeYAML :: String
} deriving (Generic)

data RowsFromYAML = RowsFromYAML{
  rowListYAML :: [RowFromYAML]
  } deriving (Generic)

data RowFromYAML = RowFromYAML{
  rowYAML :: [ValueFromYAML]
} deriving (Generic)

data ValueFromYAML = ValueFromYAML{
  valueYAML :: String
} deriving (Generic, Show)

--------------------------------------------

instance FromJSON SqlException
instance FromJSON SqlStatement
instance FromJSON SqlTableFromYAML
instance FromJSON ColumnsFromYAML
instance FromJSON RowsFromYAML
instance FromJSON ColumnFromYAML
instance FromJSON RowFromYAML
instance FromJSON ValueFromYAML

instance ToJSON SqlException
instance ToJSON SqlStatement
instance ToJSON SqlTableFromYAML
instance ToJSON ColumnsFromYAML
instance ToJSON RowsFromYAML
instance ToJSON ColumnFromYAML
instance ToJSON RowFromYAML
instance ToJSON ValueFromYAML

--------------------------------------------
toTable :: String -> Maybe SqlTableFromYAML
toTable yasm = decode $ BS.pack yasm

toStatement :: String -> Maybe SqlStatement
toStatement yasm = decode $ BS.pack yasm

toException :: String -> Maybe SqlException
toException yasm = decode $ BS.pack yasm

fromTable :: SqlTableFromYAML -> String
fromTable table = BS.unpack (encode table)

fromStatement :: SqlStatement -> String
fromStatement statement = BS.unpack (encode statement)

fromException :: SqlException -> String
fromException exception = BS.unpack (encode exception)


toDataframe :: SqlTableFromYAML -> DataFrame
toDataframe table =
  DataFrame
    (map (\col -> Column (columnNameYAML col) (toColumnType $ columnTypeYAML col)) $ columnListYAML $ columnsYAML table)
    (map (map convertToValue . rowYAML) $ rowListYAML $ rowsYAML table)


toColumnType :: String -> ColumnType
toColumnType "IntegerType" = IntegerType
toColumnType "StringType"  = StringType
toColumnType "BoolType"    = BoolType
toColumnType _         = error "Unsupported data type"

convertToValue :: ValueFromYAML -> DF.Value
convertToValue (ValueFromYAML str) =
  case splitFirstWord str of
    ("IntegerValue", val) -> DF.IntegerValue (read val)
    ("StringValue", val) -> DF.StringValue val
    ("BoolValue", "True") -> DF.BoolValue True
    ("BoolValue", "False") -> DF.BoolValue False
    ("NullValue", _) -> DF.NullValue
    _ -> error $ "Invalid FromJSONValue format: " ++ str

splitFirstWord :: String -> (String, String)
splitFirstWord str =
  case break (\c -> c == ' ' || c == '\t') str of
    (first, rest) -> (first, dropWhile (\c -> c == ' ' || c == '\t') rest)


fromDataFrame :: DataFrame -> SqlTableFromYAML
fromDataFrame (DataFrame col row) = SqlTableFromYAML {
  columnsYAML = fromColumns col,
  rowsYAML = fromRows row
}

fromColumns :: [Column] -> ColumnsFromYAML
fromColumns [] = ColumnsFromYAML { columnListYAML = [] }
fromColumns (x:xs) = ColumnsFromYAML {
  columnListYAML = fromColumn x : columnListYAML (fromColumns xs)
}

fromColumn :: Column -> ColumnFromYAML
fromColumn (Column name colType) = ColumnFromYAML {
  columnNameYAML = name,
  columnTypeYAML = show colType
}

fromRows :: [Row] -> RowsFromYAML
fromRows [] = RowsFromYAML {rowListYAML = []}
fromRows (x:xs) = RowsFromYAML {
  rowListYAML = fromRow x : rowListYAML (fromRows xs)
}


fromRow :: Row -> RowFromYAML
fromRow [] = RowFromYAML { rowYAML = [] }
fromRow (x:xs) = RowFromYAML {
  rowYAML = fromValue x : rowYAML (fromRow xs)
}


fromValue :: DF.Value -> ValueFromYAML
fromValue value = ValueFromYAML{
  valueYAML = show value
}

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



-----------------for communication ends----------------

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

------------------------------------------------------------------------------------


newtype EitherT e m a = EitherT {
    runEitherT :: m (Either e a)
}

instance MonadTrans (EitherT e) where
  lift :: Monad m => m a -> EitherT e m a
  lift ma = EitherT $ fmap Right ma

instance Monad m => Functor (EitherT e m) where
  fmap :: (a -> b) -> EitherT e m a -> EitherT e m b
  fmap f ta = EitherT $ do
    eit <- runEitherT ta
    case eit of
      Left e -> return $ Left e
      Right a -> return $ Right (f a)

instance Monad m => Applicative (EitherT e m) where
  pure :: a -> EitherT e m a
  pure a = EitherT $ return $ Right a
  (<*>) :: EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
  af <*> aa = EitherT $ do
    f <- runEitherT af
    case f of
      Left e1 -> return $ Left e1
      Right r1 -> do
        a <- runEitherT aa
        case a of
          Left e2 -> return $ Left e2
          Right r2 -> return $ Right (r1 r2)

instance (IsString e) => Alternative (EitherT e (State s)) where
    empty :: EitherT e (State s) a
    empty = EitherT $ state $ \s -> (Left (Data.String.fromString "Error"), s)

    (<|>) :: EitherT e (State s) a -> EitherT e (State s) a -> EitherT e (State s) a
    a1 <|> a2 = EitherT $ state $ \s ->
        let result = runState (runEitherT a1) s
        in case result of
            (Left _, _) -> runState (runEitherT a2) s
            _ -> result


instance Monad m => Monad (EitherT e m) where
  (>>=) :: EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
  m >>= k = EitherT $ do
    eit <- runEitherT m
    case eit of
      Left e -> return $ Left e
      Right r -> runEitherT (k r)

throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err

------------------------------------------------------------------------------------

parseStatement :: String -> Either Error ParsedStatement3
parseStatement input = do
  (remain, query) <- runParser p input
  ( _, _) <- runParser stopParseAt remain
  return query
  where
    p :: Parser4 ParsedStatement3
    p = dropTableParser
        <|> showTableParser
        <|> showTablesParser
        <|> selectStatementParser
        <|> selectAllParser
        <|> insertParser
        <|> updateParser
        <|> deleteParser
        <|> selectNowParser
        <|> createTableParser
        <|> showTableParser

--------------------------EXECUTION ALGEBRYZAS-----------------------------------

data ExecutionAlgebra next =
    GetTables (TableArray -> next)
  | LoadFile TableName (Either ErrorMessage DeserializedContent -> next)
  | SaveFile  (TableName, DataFrame) (() -> next)
  | GetTime (UTCTime -> next)
  | DropFile TableName (Either ErrorMessage () -> next)
  -- feel free to add more constructors heres 
  deriving Functor

type Execution = Free ExecutionAlgebra

---------------------------------RUN STEP'AMS------------------------------------

loadFile :: TableName -> Execution (Either ErrorMessage DeserializedContent)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getTables :: Execution TableArray
getTables = liftF $ GetTables id

saveFile :: (TableName, DataFrame) -> Execution ()
saveFile table = liftF $ SaveFile table id

dropFile :: TableName -> Execution (Either ErrorMessage ())
dropFile table = liftF $ DropFile table id 

--------------------------EXECUTE SQL REIKALIUKAI--------------------------------

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
  Right (Lib4.SelectAll tables selectWhere order) -> do
    contents <- loadFromFiles tables
    case contents of
      Right tuples -> case executeSelectAll tables (snd $ switchListToTuple' tuples) selectWhere order of
        Right dfs -> return $ Right dfs
        Left err -> return $ Left err
      Left err -> return $ Left err
  Left err -> return $ Left err

  Right (Lib4.ShowTable table) -> do
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

  Right Lib4.ShowTables -> do 
    files <- getTables
    let df = executeShowTables files
    return $ Right df

  Right (Lib4.Select specialSelect tables selectWhere order) -> do
    time <- getTime
    contents <- loadFromFiles tables
    case contents of
      Right tuples -> case executeSelectWithAllSauces specialSelect tables (snd $ switchListToTuple' tuples) selectWhere time order of
        Right dfs -> return $ Right dfs
        Left err -> return $ Left err
      Left err -> return $ Left err
  Left err -> return $ Left err

  Right (DropTable table) -> do
        drop <- dropFile table
        case drop of 
          Left err -> return $ Left $ "Table '" ++ table ++ "' does not exist."
          Right table -> return $ Right (DataFrame [] [])

  Right (CreateTable table columns) -> do
        content <- loadFile table
        case content of
            Left _ -> do
                saveFile (table, DataFrame columns [])
                return $ Right (DataFrame columns [])
            Right _ -> return $ Left "This name of table is already used." 

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

-------------------------------EXECUTIONAS---------------------------------------

executeShowTable :: DataFrame -> TableName -> DataFrame
executeShowTable df table = createColumnsDataFrame (columnsToList df) table

executeShowTables :: TableArray -> DataFrame
executeShowTables tables = createTablesDataFrame tables

executeSelectAll :: TableArray -> [DataFrame] -> Maybe WhereSelect -> Maybe OrderBy -> Either ErrorMessage DataFrame
executeSelectAll tables selectedDfs whereSelect order = case areTablesValid selectedDfs of
    True -> case whereSelect of
      Just conditions -> case isFaultyConditions conditions of
        False -> case doColumnsExistDFs (whereConditionColumnList conditions) selectedDfs of
          True -> case doColumnsExistProvidedDfs tables selectedDfs (whereConditionColumnList2 conditions) of
            True -> case checkForMatchingColumns (getAllColumnsCartesianDF (createCartesianDataFrame selectedDfs tables)) (whereConditionColumnList conditions) of
              True -> case areRowsEmpty (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) of
                False -> case order of
                  Just o ->  case sortCartesianDataFrame (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) o of
                    Left err -> Left err
                    Right cdf -> Right $ deCartesianDataFrame cdf
                  Nothing -> Right (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)
                True -> Left "There are no results with the provided conditions or the condition is faulty"
              False -> Left "Some of provided column names are ambiguous"
            False -> Left "Some of provided columns do not exist in provided tables or expected table where not provided after 'from'"
          False -> Left "Some of provided columns do not exist in provided tables"
        True -> Left "Conditions are faulty"
      Nothing -> case order of
        Just o -> case sortCartesianDataFrame (createCartesianDataFrame selectedDfs tables) o of
          Right cdf -> Right $ deCartesianDataFrame cdf
          Left err -> Left err
        Nothing -> Right $ cartesianDataFrame selectedDfs
    False -> Left "Some of provided tables are not valid"

executeSelectWithAllSauces :: SpecialSelect2 -> TableArray -> [DataFrame] -> Maybe WhereSelect -> UTCTime -> Maybe OrderBy -> Either ErrorMessage DataFrame
executeSelectWithAllSauces specialSelect tables selectedDfs whereSelect time order = case specialSelect of
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
                      Just _ -> case order of 
                        Just o -> case sortCartesianDataFrame (createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [uncurry createSelectDataFrame 
                                  $ getColumnsRows specialColumns (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)]) (["Now"] ++ tables)) o of
                          Right cdf -> Right $ deCartesianDataFrame cdf
                          Left err -> Left err
                        Nothing -> Right $ deCartesianDataFrame (createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [uncurry createSelectDataFrame 
                                  $ getColumnsRows specialColumns (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)] ) (["Now"] ++ tables))
                      Nothing -> case order of
                        Just o -> case sortCartesianDataFrame (( createCarDataFrame ( getColumnsRowsC specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)) )) o of
                          Right cdf -> Right $ deCartesianDataFrame cdf
                          Left err -> Left err
                        Nothing -> Right $ uncurry createSelectDataFrame 
                                  $ getColumnsRows specialColumns (deCartesianDataFrame $ filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)
                    True -> Left "There are no results with the provided conditions or the condition is faulty"
                  False -> Left "Some of provided column names are ambiguous"
                False -> Left "Some of provided columns do not exist in provided tables or expected table where not provided after 'from'"
              False -> Left "Some of provided columns do not exist in provided tables"
            True -> Left "Conditions are faulty"
          Nothing -> case nowFunction of
            Just _ -> case order of 
              Just o -> case sortCartesianDataFrame (createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [uncurry createSelectDataFrame $ getColumnsRows specialColumns (deCartesianDataFrame $ createCartesianDataFrame selectedDfs tables)]) (["Now"] ++ tables)) o of
                Right cdf -> Right $ deCartesianDataFrame cdf
                Left err -> Left err
              Nothing -> Right $ deCartesianDataFrame (createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [uncurry createSelectDataFrame $ getColumnsRows specialColumns (deCartesianDataFrame $ createCartesianDataFrame selectedDfs tables)]) (["Now"] ++ tables))
            Nothing -> case order of
              Just o -> case sortCartesianDataFrame (createCarDataFrame (getColumnsRowsC specialColumns (createCartesianDataFrame selectedDfs tables))) o of
                Right cdf -> Right $ deCartesianDataFrame cdf
                Left err -> Left err
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
                  Just _ -> case order of
                    Just o -> case sortCartesianDataFrame (createCartesianDataFrame( [createNowDataFrame (uTCToString time)] ++ [uncurry createSelectDataFrame 
                              $ (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions), snd $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions))]) (["Now"] ++ tables)) o of
                      Right cdf -> Right $ deCartesianDataFrame cdf
                      Left err -> Left err
                    Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [uncurry createSelectDataFrame 
                              $ (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions), snd $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions))]) (["Now"] ++ tables)
                  Nothing -> case order of
                    Just o -> case sortCartesianDataFrame (createCarDataFrame 
                              $  getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions)) o of
                      Right cdf -> Right $ deCartesianDataFrame cdf
                      Left err -> Left err
                    Nothing -> Right $ uncurry createSelectDataFrame 
                              $ (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions), snd $ getColumnsTablesList specialColumns (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions))
                True -> Left "There are no results with the provided conditions or the condition is faulty"
              False -> Left "Some of provided column names are ambiguous"
            False -> Left "Some of provided columns do not exist in provided tables"
          True -> Left "Conditions are faulty"
        Nothing -> case nowFunction of
          Just _ -> case order of
            Just o -> case sortCartesianDataFrame (createCartesianDataFrame ([uncurry createSelectDataFrame 
                              (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables), snd $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables))] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])) o of
              Right cdf -> Right $ deCartesianDataFrame cdf
              Left err -> Left err
            Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([uncurry createSelectDataFrame 
                              (deCartesianColumns $ fst $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables), snd $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables))] ++ [createNowDataFrame (uTCToString time)]) (tables ++ ["Now"])
          Nothing -> case order of
            Just o -> case sortCartesianDataFrame (createCarDataFrame 
                              $ getColumnsTablesList specialColumns (createCartesianDataFrame selectedDfs tables)) o of
              Right cdf -> Right $ deCartesianDataFrame cdf
              Left err -> Left err 
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
                              False -> case order of
                                Just o -> case validateOrderBy o (createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols2 [rows2]]) (["Now"] ++ tables)) of
                                  True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols2 [rows2]]) (["Now"] ++ tables)
                                  False -> Left "Provided order by columns are faulty"
                                Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols2 [rows2]]) (["Now"] ++ tables)
                            Left err -> Left err
                          False -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> case order of
                                Just o -> case validateOrderBy o (createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols [rows]]) (["Now"] ++ tables)) of
                                  True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols [rows]]) (["Now"] ++ tables)
                                  False -> Left "Provided order by columns are faulty"
                                Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols [rows]]) (["Now"] ++ tables)
                              False -> case order of
                                Just o -> case validateOrderBy o $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame (cols ++ cols2) [rows ++ rows2]]) (["Now"] ++ tables) of
                                  True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame (cols ++ cols2) [rows ++ rows2]]) (["Now"] ++ tables) 
                                  False -> Left "Provided order by columns are faulty"
                                Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame (cols ++ cols2) [rows ++ rows2]]) (["Now"] ++ tables) 
                            Left err -> Left err
                        Left err -> Left err
                      Nothing -> case processSelect2 (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                        Right (cols, rows) -> case null cols of
                          True -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> Left "There are no results"
                              False -> case order of
                                Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                                  True -> Right $ createSelectDataFrame cols2 [rows2]
                                  False -> Left "Provided order by columns are faulty" 
                                Nothing -> Right $ createSelectDataFrame cols2 [rows2]
                            Left err -> Left err
                          False -> case processSelect2' (filterSelectAll (createCartesianDataFrame selectedDfs tables) conditions) aggregatesList of
                            Right (cols2, rows2) -> case null cols2 of
                              True -> case order of
                                Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                                  True -> Right $ createSelectDataFrame cols [rows]
                                  False -> Left "Provided order by columns are faulty" 
                                Nothing -> Right $ createSelectDataFrame cols [rows]
                              False -> case order of
                                Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                                  True -> Right $ createSelectDataFrame (cols ++ cols2) [rows ++ rows2]
                                  False -> Left "Provided order by columns are faulty"
                                Nothing -> Right $ createSelectDataFrame (cols ++ cols2) [rows ++ rows2]
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
                      False -> case order of
                        Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                          True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols2 [rows2]]) (["Now"] ++ tables) 
                          False -> Left "Provided order by columns are faulty" 
                        Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols2 [rows2]]) (["Now"] ++ tables) 
                    Left err -> Left err
                  False -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> case order of
                        Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                          True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols [rows]]) (["Now"] ++ tables)
                          False ->  Left "Provided order by columns are faulty" 
                        Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame cols [rows]]) (["Now"] ++ tables) 
                      False -> case order of
                        Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                          True -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame (cols ++ cols2) [rows ++ rows2]]) (["Now"] ++ tables) 
                          False -> Left "Provided order by columns are faulty" 
                        Nothing -> Right $ deCartesianDataFrame $ createCartesianDataFrame ([createNowDataFrame (uTCToString time)] ++ [createSelectDataFrame (cols ++ cols2) [rows ++ rows2]]) (["Now"] ++ tables) 
                    Left err -> Left err
                Left err -> Left err
              Nothing -> case processSelect2 (createCartesianDataFrame selectedDfs tables) aggregatesList of
                Right (cols, rows) -> case null cols of
                  True -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> Left "There are no results"
                      False -> case order of
                        Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                          True -> Right $ createSelectDataFrame cols2 [rows2]
                          False -> Left "Provided order by columns are faulty" 
                        Nothing -> Right $ createSelectDataFrame cols2 [rows2]
                    Left err -> Left err
                  False -> case processSelect2' (createCartesianDataFrame selectedDfs tables) aggregatesList of
                    Right (cols2, rows2) -> case null cols2 of
                      True -> case order of
                        Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                          True -> Right $ createSelectDataFrame cols [rows]
                          False ->  Left "Provided order by columns are faulty" 
                        Nothing -> Right $ createSelectDataFrame cols [rows]
                      False -> case order of
                        Just o -> case validationAggregatesOrderBy tables selectedDfs (getColumnsFromAggregates aggregatesList) o of
                          True -> Right $ createSelectDataFrame (cols ++ cols2) [rows ++ rows2]
                          False ->  Left "Provided order by columns are faulty" 
                        Nothing -> Right $ createSelectDataFrame (cols ++ cols2) [rows ++ rows2]
                    Left err -> Left err
                Left err -> Left err
          False -> Left "Some of provided columns in aggregate functions do not exist in provided tables"
        False -> Left "Some of provided columns in aggregate functions are ambiguous" 
      False -> Left "Some of provided columns in aggregate functions do not exist in provided tables" 
    False -> Left "Some of provided table(s) are not valid"

-------------------------------------------NEW STUFF FOR EXECUTION-------------------------------------------------

getColumnsRowsC :: [ColumnName] -> CartesianDataFrame -> ([CartesianColumn], [Row])
getColumnsRowsC colList (CartesianDataFrame col row) = (getColumnListC (getTableNamesC col) colList (getColumnTypeC colList col) , getNewRowsC col row colList)

getNewRowsC :: [CartesianColumn] -> [Row] -> [ColumnName] -> [Row]
getNewRowsC _ [] _ = []
getNewRowsC cols (x:xs) colNames = getNewRowC x cols colNames : getNewRowsC cols xs colNames

getNewRowC :: [DF.Value] -> [CartesianColumn] -> [ColumnName] -> [DF.Value]
getNewRowC _ _ [] = []
getNewRowC row cols (x:xs) = getValueFromRowC row (findColumnIndexC x cols) 0 : getNewRowC row cols xs

getValueFromRowC :: Row -> Int -> Int -> DF.Value
getValueFromRowC (x:xs) index i
  | index == i = x
  | otherwise = getValueFromRowC xs index (i+1)

getColumnTypeC :: [ColumnName] -> [CartesianColumn] -> [ColumnType]
getColumnTypeC [] _ = []
getColumnTypeC (x:xs) col = columnTypeC col 0 (findColumnIndexC x col) : getColumnTypeC xs col

columnTypeC :: [CartesianColumn] -> Int -> Int -> ColumnType
columnTypeC (x:xs) i colIndex
  | i == colIndex = getTypeC x
  | otherwise = columnTypeC xs (i+1) colIndex

getTypeC :: CartesianColumn -> ColumnType
getTypeC (CartesianColumn (_, Column _ colType)) = colType


getColumnListC :: [TableName] -> [ColumnName] -> [ColumnType] -> [CartesianColumn]
getColumnListC [] [] [] = []
getColumnListC _ [] _ = []
getColumnListC [] _ _ = []
getColumnListC _ _ [] = []
getColumnListC (z:zs) (x:xs) (y:ys) = CartesianColumn (z, Column x y) : getColumnListC zs xs ys

findColumnIndexC :: ColumnName -> [CartesianColumn] -> Int
findColumnIndexC columnName columns = columnIndexC columnName columns 0

columnIndexC :: ColumnName -> [CartesianColumn] -> Int -> Int
columnIndexC _ [] _ = -1
columnIndexC columnName ((CartesianColumn (_, Column name _)):xs) index
    | columnName /= name = (columnIndexC columnName xs (index + 1))
    | otherwise = index

getTableNamesC :: [CartesianColumn] -> [TableName]
getTableNamesC [] = []
getTableNamesC ((CartesianColumn (table, _)):xs) = [table] ++ getTableNamesC xs

createCarDataFrame :: ([CartesianColumn], [Row]) -> CartesianDataFrame
createCarDataFrame (columns, rows) = CartesianDataFrame columns rows

validationAggregatesOrderBy :: TableArray -> [DataFrame] -> [ColumnName] -> [(OrderByValue, AscDesc)] -> Bool
validationAggregatesOrderBy _ _ _ [] = True
validationAggregatesOrderBy tables dfs names ((order, _):xs) = case order of
  ColumnNumber number -> if fromInteger number > length names || number < 1 then False else validationAggregatesOrderBy tables dfs names xs
  ColumnName name -> if (elem name names) then validationAggregatesOrderBy tables dfs names xs else False
  ColumnTable (table, name) -> if doColumnsExistProvidedDfs tables dfs [(table, name)] == True then validationAggregatesOrderBy tables dfs names xs else False
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

---------------------------------------some now() stuff here-----------------------------

createNowDataFrame :: [Row] -> DataFrame
createNowDataFrame time = DataFrame [Column "Now" StringType] time

uTCToString :: UTCTime -> [Row]
uTCToString utcTime = [[StringValue (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" (addUTCTime (120*60) utcTime))]]

-----------------------------------------------------------------------------------

dropTableParser :: Parser4 ParsedStatement3
dropTableParser = do
    _ <- queryStatementParser "drop"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser 
    pure $ DropTable table

createTableParser :: Parser4 ParsedStatement3
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

runParser :: Parser4 a -> String -> Either Error (String, a)
runParser parser input = 
    let eitherResult = runState (runEitherT parser) input
    in case eitherResult of
        (Left errMsg, _) -> Left errMsg
        (Right value, remainingInput) -> Right (remainingInput, value)

----------------------------------------------------------------------------------

insertParser :: Parser4 ParsedStatement3
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

selectDataParsers :: Parser4 SelectedColumns
selectDataParsers = tryParseColumn
  where
    tryParseColumn = do
      _ <- queryStatementParser "("
      columnNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      _ <- queryStatementParser ")"
      return $ ColumnsSelected columnNames

insertedValuesParser :: Parser4 InsertedValues
insertedValuesParser = do
  values <- seperate valueParser (queryStatementParser "," >> optional whitespaceParser)
  return values

valueParser :: Parser4 DF.Value
valueParser = parseNullValue <|> parseBoolValue <|> parseStringValue <|> parseNumericValue

parseNullValue :: Parser4 DF.Value
parseNullValue = queryStatementParser "null" >> return NullValue

parseBoolValue :: Parser4 DF.Value
parseBoolValue =
  (queryStatementParser "true" >> return (BoolValue True))
  <|> (queryStatementParser "false" >> return (BoolValue False))

parseStringValue :: Parser4 DF.Value
parseStringValue = do
  strValue <- parseStringWithQuotes
  return (StringValue strValue)

parseStringWithQuotes :: Parser4 String
parseStringWithQuotes = do
  _ <- queryStatementParser "'"
  str <- some (parseSatisfy (/= '\''))
  _ <- queryStatementParser "'"
  return str

parseNumericValue :: Parser4 DF.Value
parseNumericValue = IntegerValue <$> parseInt

parseInt :: Parser4 Integer
parseInt = do
  digits <- some (parseSatisfy isDigit)
  return (read digits)

deleteParser :: Parser4 ParsedStatement3
deleteParser = do
    _ <- queryStatementParser "delete"
    _ <- whitespaceParser
    _ <- queryStatementParser "from"
    _ <- whitespaceParser
    tableName <- columnNameParser
    conditions <- optional whereParser
    pure $ Delete tableName conditions

updateParser :: Parser4 ParsedStatement3
updateParser = do
    _ <- queryStatementParser "update"
    _ <- whitespaceParser
    tableName <- columnNameParser
    selectUpdated <- setParser
    selectedWhere <- optional whereParser
    pure $ Update tableName selectUpdated selectedWhere

setParser :: Parser4 WhereSelect
setParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "set"
  _ <- whitespaceParser
  seperate whereConditionParser (optional whitespaceParser >> char ',' >> optional whitespaceParser)


selectAllParser :: Parser4 ParsedStatement3
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
  orderBy <- optional orderByParser
  _ <- optional whitespaceParser
  pure $ SelectAll tableArray selectWhere orderBy

--------------------------order by----------------------------------
orderByParser :: Parser4 OrderBy
orderByParser = do
  order <- queryStatementParser "order"
  _ <- whitespaceParser
  by <- queryStatementParser "by"
  _ <- whitespaceParser
  some orderValuesParser
  where 
    orderValuesParser :: Parser4 (OrderByValue, AscDesc)
    orderValuesParser = do
      value <- orderByValueParser
      ascDesc <- ascDescParser
      _ <- optional (char ',' >> optional whitespaceParser)
      pure (value, ascDesc)

orderByValueParser :: Parser4 OrderByValue
orderByValueParser = do (ColumnNumber <$> numberParser) <|> (ColumnTable <$> columnNameTableParser) <|> (ColumnName <$> columnNameParser)

ascDescParser :: Parser4 AscDesc
ascDescParser = tryParseDesc <|> tryParseAsc <|> tryParseSymbol
  where
    tryParseDesc = do
      _ <- whitespaceParser
      _ <- queryStatementParser "desc"
      return $ Desc "desc"
    tryParseAsc = do
      _ <- whitespaceParser
      _ <- queryStatementParser "asc"
      return $ Asc "asc"
    tryParseSymbol = do
      _ <- optional whitespaceParser
      return $ Asc "asc"

validateOrderBy :: [(OrderByValue, AscDesc)] -> CartesianDataFrame -> Bool
validateOrderBy [] _ = True
validateOrderBy ((order, _):xs) (CartesianDataFrame cols rows) = case order of
  ColumnTable _ -> case validateColumnTable cols order of
    True -> validateOrderBy xs (CartesianDataFrame cols rows)
    False -> False
  ColumnName name -> case validateColumnName name (deCartesianColumns cols) of
    True -> validateOrderBy xs (CartesianDataFrame cols rows)
    False -> False
  ColumnNumber number -> case validateColumnNumber (fromInteger number) cols of
    True -> validateOrderBy xs (CartesianDataFrame cols rows)
    False -> False

----------------------------------------------------------------
toAscDescList :: OrderBy -> [AscDesc]
toAscDescList ((orderByValue, ascdesc):xs)
  | xs /= [] = [ascdesc] ++ toAscDescList xs
  | otherwise = [ascdesc] 

validateColumnNumber :: Int ->  [CartesianColumn] -> Bool
validateColumnNumber int cols = 
  case int > (length cols) - 1 || int < 0 of
    True -> False
    False -> True

validateColumnName :: ColumnName -> [Column] -> Bool
validateColumnName name cols = 
  if (countOfName name cols 0) == 1
    then True
    else False

countOfName :: ColumnName -> [Column] -> Int -> Int
countOfName _ [] index = index
countOfName name ((Column aName _ ):xs) index = 
  if aName == name 
    then (index + 1) + countOfName name xs index
    else index + countOfName name xs index

validateColumnTable :: [CartesianColumn] -> OrderByValue -> Bool
validateColumnTable [] _ = False
validateColumnTable cc@((CartesianColumn (table, Column name _)):xs) (ColumnTable (tableOr, columnOr)) = 
  case table == tableOr && name == columnOr of
    False -> validateColumnTable cc (ColumnTable (tableOr, columnOr))
    True -> case countOfColumnTable cc (ColumnTable (tableOr, columnOr)) 0 of
      1 -> True
      _ -> False

countOfColumnTable :: [CartesianColumn] -> OrderByValue -> Int -> Int
countOfColumnTable [] _ i = i
countOfColumnTable ((CartesianColumn (table, Column name _)):xs) (ColumnTable (tableOr, columnOr)) i =
  case table == tableOr && name == columnOr of
    True -> countOfColumnTable xs (ColumnTable (tableOr, columnOr)) (i + 1)
    False -> countOfColumnTable xs (ColumnTable (tableOr, columnOr)) i


toCartesianColumns :: CartesianDataFrame -> [CartesianColumn]
toCartesianColumns (CartesianDataFrame cols rows) = cols
--------------------------------------SORT------------------------------------

sortCartesianDataFrame :: CartesianDataFrame -> OrderBy -> Either ErrorMessage CartesianDataFrame
sortCartesianDataFrame (CartesianDataFrame cols rows) order = do
  sortByValues <- mapM (getValuesForSort (CartesianDataFrame cols rows)) order
  let sortedRows = sortBy (compareWhenEqual sortByValues) rows
  return $ CartesianDataFrame cols sortedRows

getValuesForSort :: CartesianDataFrame -> (OrderByValue, AscDesc) -> Either ErrorMessage (Int, AscDesc)
getValuesForSort (cdf@(CartesianDataFrame cols _)) (order, ascdesc) = case order of
  ColumnNumber _ -> Right ((getColumnNumberFromOrderBy (order, ascdesc))-1, ascdesc)
  ColumnName _ -> Right ((findColumnIndex (getColumnNameFromOrderBy (order, ascdesc)) (deCartesianColumns (toCartesianColumns cdf))), ascdesc)
  ColumnTable (table, name) -> Right (findColumnTableIndex name table cols, ascdesc)

compareWhenEqual :: [(Int, AscDesc)] -> Row -> Row -> Ordering
compareWhenEqual [] _ _ = EQ
compareWhenEqual ((i, ascdesc):iascdesc) row1 row2 =
    let priority = compareValues ascdesc (row1 !! i) (row2 !! i)
    in if priority == EQ then compareWhenEqual (adjustIndices iascdesc) row1 row2 else priority
  where
    adjustIndices = map (\(idx, desc) -> (if idx > i then idx - 1 else idx, desc))

compareValues :: AscDesc -> DF.Value -> DF.Value -> Ordering
compareValues (Asc "asc") v1 v2 = compare v1 v2
compareValues (Desc "desc") v1 v2 = compare v2 v1

isColumnTable :: OrderByValue -> Bool
isColumnTable (ColumnTable _) = True
isColumnTable _ = False

isColumnName :: OrderByValue -> Bool
isColumnName (ColumnName _) = True
isColumnName _ = False

getColumnNameFromOrderBy :: (OrderByValue, AscDesc) -> ColumnName
getColumnNameFromOrderBy (ColumnName name ,ascdesc) = name

getColumnNumberFromOrderBy :: (OrderByValue, AscDesc) -> Int
getColumnNumberFromOrderBy (ColumnNumber int ,ascdesc) = fromInteger int

getColumnTableFromOrderBy :: (OrderByValue, AscDesc) -> CartesianDataFrame -> Int
getColumnTableFromOrderBy (ColumnTable (tablename, columnname), _) (CartesianDataFrame cols rows) = findColumnTableIndex columnname tablename cols

-----------------------------------------------------------------

selectNowParser :: Parser4 ParsedStatement3
selectNowParser = do
    _ <- queryStatementParser "select"
    _ <- whitespaceParser
    _ <- queryStatementParser "now"
    _ <- optional whitespaceParser
    _ <- queryStatementParser "("
    _ <- optional whitespaceParser
    _ <- queryStatementParser ")"
    pure SelectNow


selectStatementParser :: Parser4 ParsedStatement3
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
    orderBy <- optional orderByParser
    _ <- optional whitespaceParser
    pure $ Select specialSelect tableArray selectWhere orderBy


selectDataParser :: Parser4 SpecialSelect2
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

aggregateParser' :: Parser4 Aggregate2
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

nowParser :: Parser4 NowFunction
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

whereParser :: Parser4 WhereSelect
whereParser = do
  _ <- whitespaceParser
  _ <- queryStatementParser "where"
  _ <- whitespaceParser
  some whereAndExist
  where
    whereAndExist :: Parser4 Condition
    whereAndExist = do
      condition <- whereConditionParser
      _ <- optional (whitespaceParser >> andParser)
      pure condition

andParser :: Parser4 And
andParser = queryStatementParser "and" >> pure And

whereConditionParser :: Parser4 Condition
whereConditionParser = do
  _ <- optional whitespaceParser
  operand1 <- operandParser
  _ <- optional whitespaceParser
  operator <- operatorParser
  _ <- optional whitespaceParser
  operand2 <- operandParser
  return $ Condition operand1 operator operand2

operandParser :: Parser4 Operand
operandParser = (ConstantOperand <$> constantParser)
               <|> (ColumnTableOperand <$> columnNameTableParser)
               <|> (ColumnOperand <$> columnNameParser)

columnNameTableParser :: Parser4 (TableName, ColumnName)
columnNameTableParser = do
  table <- columnNameParser
  _ <- char '.'
  column <- columnNameParser
  return (table, column)

-------------------------------------------------------------------------------

selectTablesParser :: Parser4 TableArray
selectTablesParser = tryParseTable
  where
    tryParseTable = do
      tableNames <- seperate columnNameParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)
      return $ tableNames

showTablesParser :: Parser4 ParsedStatement3
showTablesParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "tables"
    _ <- optional whitespaceParser
    pure Lib4.ShowTables

-----------------------------------------------------------------------------------------------------------

showTableParser :: Parser4 ParsedStatement3
showTableParser = do
    _ <- queryStatementParser "show"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    pure $ Lib4.ShowTable table

columnListParser :: Parser4 [Column]
columnListParser = seperate columnAndTypeParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)

columnAndTypeParser :: Parser4 Column
columnAndTypeParser = do
    columnName <- columnNameParser
    _ <- whitespaceParser
    columnType <- columnNameParser >>= either (throwE . show) pure . columnTypeParser
    pure (Column columnName columnType)

columnTypeParser :: String -> Either Error ColumnType
columnTypeParser "int" = Right IntegerType
columnTypeParser "varchar" = Right StringType
columnTypeParser "bool" = Right BoolType
columnTypeParser other = Left $ "There is no such type as: " ++ other


parseSatisfy :: (Char -> Bool) -> Parser4 Char
parseSatisfy predicate = EitherT $ state $ \inp ->
    case inp of
        [] -> (Left "Empty input", inp)
        (x:xs) -> if predicate x 
                then (Right x, xs) 
                else (Left ("Unexpected character: " ++ [x]), inp)

queryStatementParser :: String -> Parser4 String
queryStatementParser keyword = do
    inp <- lift get  
    let len = length keyword
    case splitAt len inp of
        ([], _) -> throwE "Empty input"  
        (xs, rest) 
            | map toLower xs == map toLower keyword -> do
                lift $ put rest 
                return xs
            | otherwise -> throwE $ "Expected " ++ keyword ++ ", but found " ++ xs


whitespaceParser :: Parser4 String
whitespaceParser = do
  input <- lift get
  case span isSpace input of
    ("",_) -> throwE $ "Expected whitespace before: " ++ input
    (ws, remain) -> do
      lift $ put remain
      return ws

seperate :: Parser4 a -> Parser4 b -> Parser4 [a]
seperate p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

columnNameParser :: Parser4 ColumnName
columnNameParser = do
    inp <- lift get  
    let word = takeWhile (\x -> isAlphaNum x || x == '_') inp
    case word of
        [] -> throwE "Empty input"
        xs -> do
            lift $ put $ drop (length xs) inp 
            return xs

char :: Char -> Parser4 Char
char a = do
    inp <- lift get
    case inp of
        [] -> throwE "Empty input"
        (x:xs) -> if a == x then do
                                lift $ put xs
                                return a
                            else throwE ([a] ++ " expected but " ++ [x] ++ " found")

numberParser :: Parser4 Integer
numberParser = do
  input <- lift get
  case takeWhile (\x -> isDigit x) input of
    [] -> throwE "Empty input"
    xs -> do
      lift $ put $ drop (length xs) input
      return $ stringToInt xs

trashParser :: Parser4 Trash 
trashParser = do
  input <- lift get
  case head input == ',' of 
    True -> throwE "Columns are not listed right"
    False -> case "from" `isPrefixOf` (dropWhiteSpaces input) of 
      True -> do
                lift $ put input
                return $ Trash ""
      False -> throwE "Columns are not listed right or aggregate functions and column names cannot be mixed"

constantParser :: Parser4 DF.Value
constantParser = do
  input <- lift get
  case input == "" of 
    True -> throwE "The query does not end with a ;"
    False -> 
      let operand = getOperand input
          restQuery = drop (length operand) input
      in case head input == ';' of
          False -> case head operand == '\'' && last operand == '\'' of
            True -> do
                      lift $ put restQuery
                      return $ StringValue (init (tail operand))
            False -> case operand of
              "True" -> do
                          lift $ put restQuery
                          return $ BoolValue True
              "False" -> do
                          lift $ put restQuery
                          return $ BoolValue False
              "null" -> do
                          lift $ put restQuery
                          return $ NullValue
              _ ->  if isNumber operand
                    then do 
                            lift $ put restQuery
                            return $ IntegerValue $ stringToInt operand
                    else throwE "Operand is not valid"
          True -> throwE "The conditions are missing"


columnNameParser' :: Parser4 ColumnName
columnNameParser' = do
  input <- lift get
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses input)) 
    then do
            lift $ put $ snd (splitStatementAtParentheses input) 
            return $ dropWhiteSpaces (fst (splitStatementAtParentheses input))
  else throwE "There is more than one column name in aggregation function")


columnNameParser'' :: Parser4 ColumnName
columnNameParser'' = do
  input <- lift get
  (if areSpacesBetweenWords (fst (splitStatementAtParentheses input)) 
    then do
            lift $ put $ snd (splitStatementAtParentheses input)
            return $ dropWhiteSpaces (fst (splitStatementAtParentheses input))
  else throwE "There is more than one column name in aggregation function")

operatorParser :: Parser4 Operator
operatorParser = 
  (queryStatementParser "=" >> pure IsEqualTo)
  <|> (queryStatementParser "!=" >> pure IsNotEqual)
  <|> (queryStatementParser "<" >> pure IsLessThan)
  <|> (queryStatementParser ">" >> pure IsGreaterThan)
  <|> (queryStatementParser "<=" >> pure IsLessOrEqual)
  <|> (queryStatementParser ">=" >> pure IsGreaterOrEqual)

aggregateFunctionParser :: Parser4 AggregateFunction
aggregateFunctionParser = sumParser <|> maxParser
  where
    sumParser = do
        _ <- queryStatementParser "sum"
        pure Sum
    maxParser = do
        _ <- queryStatementParser "max"
        pure Max

stopParseAt :: Parser4 String
stopParseAt  = do
  _ <- optional whitespaceParser
  _ <- queryStatementParser ";"
  checkAfterQuery
  where
    checkAfterQuery :: Parser4 String
    checkAfterQuery = do
      query <- lift get
      case query of
          [] -> do
                  lift $ put []
                  return []
          s -> throwE ("Characters found after ;" ++ s)

----------------------------LIB3 STUFF-------------------------

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
columnTableIndex _ _ [] index = -1
columnTableIndex columnName tableName ((CartesianColumn (table, (Column name _))):xs) index
    | columnName /= name || tableName /= table = (columnTableIndex columnName tableName xs (index + 1))
    | otherwise = index

getColumnListFromCartesianDF :: [CartesianColumn] -> [Column]
getColumnListFromCartesianDF [] = []
getColumnListFromCartesianDF ((CartesianColumn (table, column)):xs) = [column] ++ getColumnListFromCartesianDF xs

----------------------------end of select execute utillities--------------------