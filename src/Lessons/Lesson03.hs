module Lessons.Lesson03 () where
import Data.Char

database :: [(String, Int)]
database = [
    ("vienas", 1),
    ("du", 2),
    ("trys", 3)]

search :: [(String, Int)] -> String -> Maybe Int
search [] _ = Nothing
search ((k,v):xs) key =
    if key == k then Just v else search xs key

extractOrDefault :: Maybe a -> a -> a
extractOrDefault (Just a) _ = a
extractOrDefault Nothing d = d

type ParseError = String

parseQuery :: String -> Either ParseError String
parseQuery l1 =
    case parseSelect l1 of
        Left e -> Left e
        Right l2 -> case parseWhitespace l2 of
            Left e -> Left e
            Right l3 -> case parseColumnList l3 of
                Left e -> Left e
                Right l4 -> case parseWhitespace l4 of
                    Left e -> Left e
                    Right l5 -> Right l5

parseWhitespace :: String -> Either ParseError String
parseWhitespace = parseChar ' '


parseColumnList :: String -> Either ParseError String
parseColumnList = parseChar '*'

parseSelect :: String -> Either ParseError String
parseSelect l1 =
    case parseChar 's' l1 of
        Left e -> Left e
        Right l2 -> case parseChar 'e' l2 of
            Left e -> Left e
            Right l3 -> case parseChar 'l' l3 of
                Left e -> Left e
                Right l4 -> Right l4

addOne :: Int -> Int
addOne a = a + 1


-- case insensitive
parseChar :: Char -> String -> Either ParseError String
parseChar _ [] = Left "Empty input"
parseChar a (x:xs) =
    if a == x || toUpper a == x then Right xs else Left ([a] ++ " expected but " ++ [x] ++ " found")
