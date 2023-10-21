{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName database tableName = findTableByName' database (lowerCaseString tableName)
    where
        findTableByName' :: Database -> String -> Maybe DataFrame
        findTableByName' [] _ = Nothing
        findTableByName' (x:xs) lowerTableName
            | lowerCaseString (fst x) == lowerTableName = (Just (snd x))
            | otherwise = findTableByName' xs lowerTableName

        lowerCaseString :: String -> String
        lowerCaseString [] = []
        lowerCaseString (x:xs)
            | isLowerCase x == True = toEnum (fromEnum x + 32) : lowerCaseString xs
            | otherwise = x : lowerCaseString xs

        isLowerCase :: Char -> Bool
        isLowerCase c = if c >= 'A' && c <= 'Z' then True else False 


-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement "" = Left "Empty input"
parseSelectAllStatement selectStatement1 =
    case parseStatement selectStatement1 of
        Left e -> Left e
        Right selectStatement -> Right selectStatement
  where
    parseStatement :: String -> Either ErrorMessage String
    parseStatement input =
        case parseSelect input of
            Left e -> Left e
            Right rest1 ->
                case parseWhitespace rest1 of
                    Left e -> Left e
                    Right rest2 ->
                        case parseColumnList rest2 of
                            Left e -> Left e
                            Right rest3 ->
                                case parseWhitespace rest3 of
                                    Left e -> Left e
                                    Right rest4 ->
                                        case parseFrom rest4 of
                                            Left e -> Left e
                                            Right rest5 ->
                                                case parseWhitespace rest5 of
                                                    Left e -> Left e
                                                    Right rest6 -> Right (removeTrailingSemicolon rest6)

    parseWhitespace :: String -> Either ErrorMessage String
    parseWhitespace s = case span customIsSpace s of
        ("", _) -> Left "Expected whitespace"
        (_, rest) -> Right rest

    parseColumnList :: String -> Either ErrorMessage String
    parseColumnList ('*':rest) = Right rest
    parseColumnList _ = Left "Expected '*'"

    parseFrom :: String -> Either ErrorMessage String
    parseFrom s = case stripPrefix "from" (makeLowerCaseString s) of
        Just rest -> Right rest
        Nothing -> Left "Expected 'from'"

    parseSelect :: String -> Either ErrorMessage String
    parseSelect s = case stripPrefix "select" (makeLowerCaseString s) of
        Just rest -> Right rest
        Nothing -> Left "Expected 'select'"

    makeLowerCaseString :: String -> String
    makeLowerCaseString = map toLower

    toLower :: Char -> Char
    toLower c
        | c >= 'A' && c <= 'Z' = toEnum (fromEnum c + 32)
        | otherwise = c

    removeTrailingSemicolon :: String -> String
    removeTrailingSemicolon s
        | not (null s) && last s == ';' = init s
        | otherwise = s

    customIsSpace :: Char -> Bool
    customIsSpace c
        | c == ' '  = True
        | otherwise = False

    stripPrefix :: Eq a => [a] -> [a] -> Maybe [a]
    stripPrefix [] xs = Just xs
    stripPrefix (_:_) [] = Nothing
    stripPrefix (p:prefix) (x:xs)
        | p == x = stripPrefix prefix xs
        | otherwise = Nothing

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame cols rows) = validateRows rows
    where
        validateRows :: [Row] -> Either ErrorMessage ()
        validateRows [] = Right ()
        validateRows (r:rs) = 
            case validateRow cols r of
                Left err -> Left err
                Right _ -> validateRows rs

        validateRow :: [Column] -> [Value] -> Either ErrorMessage ()
        validateRow [] [] = Right ()
        validateRow (_:_) [] = Left "Row does not have enough values."
        validateRow [] (_:_) = Left "Row has too many values."
        validateRow (c:cs) (v:vs)
            | doesValueTypeMatchColType c v = validateRow cs vs
            | otherwise = Left "Cell's value type does not match the column's type."

        doesValueTypeMatchColType :: Column -> Value -> Bool
        doesValueTypeMatchColType _ NullValue = True
        doesValueTypeMatchColType col val =
            case getColTypeByValue val of
                Left _ -> False
                Right colType -> colType == getColType col

        getColTypeByValue :: Value -> Either ErrorMessage ColumnType
        getColTypeByValue (IntegerValue _) = Right IntegerType
        getColTypeByValue (StringValue _) = Right StringType
        getColTypeByValue (BoolValue _) = Right BoolType
        getColTypeByValue _ = Left "Unknown column type."

        getColType :: Column -> ColumnType
        getColType (Column _ t) = t

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable n (DataFrame columns rows) = header ++ "\n" ++ allRows
    where
        cWidths = calculateCWidths n columns
        header = makeHeader columns cWidths
        allRows = makeRows rows cWidths

        calculateCWidths :: Integer -> [Column] -> [Int]
        calculateCWidths maxWidth columnArray = 
            let columnNum = length columnArray
                cellWidth = maxWidth `div` fromIntegral columnNum
            in replicate columnNum (fromIntegral cellWidth)

        makeHeader :: [Column] -> [Int] -> String
        makeHeader allColumns cWidthArray =
            let headerCells = zipWith makeHCell allColumns cWidthArray
            in concat headerCells

        makeRows :: [Row] -> [Int] -> String
        makeRows rowArray cWidth = 
            let readyRows = map (makeSingleRow cWidth) rowArray
            in unlines readyRows

        makeSingleRow :: [Int] -> Row -> String
        makeSingleRow widths row = 
            let cells = zipWith makeCell row widths
            in concat cells

        makeHCell :: Column -> Int -> String
        makeHCell (Column name _) w = makeCell (StringValue name) w

        makeCell :: Value -> Int -> String
        makeCell value width = 
            let cellContent = case value of
                    StringValue s -> s 
                    IntegerValue i -> show i
                    BoolValue b -> if b then "True" else "False"
                    NullValue-> ""
                strLen = length cellContent
                emptySpace = width - strLen - 1
            in "|" ++ cellContent ++ replicate emptySpace ' '

    