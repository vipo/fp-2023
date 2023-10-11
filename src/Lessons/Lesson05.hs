{-# LANGUAGE InstanceSigs #-}

module Lessons.Lesson05 () where



e0 :: Either String Integer
e0 = Right 11

e1 :: Either String Integer
e1 = Right 4

e2 :: Either String Integer
e2 = Left "ojoj"

addOne :: Int -> Int
addOne a = a + 1


f1 :: String -> Int
f1 = addOne . length

type ErrorMessage = String

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
  fmap :: (a -> b) -> Parser a -> Parser b
  fmap f functor = Parser $ \inp ->
    case runParser functor inp of
        Left e -> Left e
        Right (l, a) -> Right (l, f a)

instance Applicative Parser where
  pure :: a -> Parser a
  pure a = Parser $ \inp -> Right (inp, a)
  (<*>) :: Parser (a -> b) -> Parser a -> Parser b
  ff <*> fa = Parser $ \in1 ->
    case runParser ff in1 of
        Left e1 -> Left e1
        Right (in2, f) -> case runParser fa in2 of
            Left e2 -> Left e2
            Right (in3, a) -> Right (in3, f a)

instance Monad Parser where
  (>>=) :: Parser a -> (a -> Parser b) -> Parser b
  ma >>= mf = Parser $ \inp1 ->
    case runParser ma inp1 of
        Left e1 -> Left e1
        Right (inp2, a) -> case runParser (mf a ) inp2 of
            Left e2 -> Left e2
            Right (inp3, r) -> Right (inp3, r)

parseChar :: Char -> Parser Char
parseChar a = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if a == x then Right (xs, a) else Left ([a] ++ " expected but " ++ [x] ++ " found")


parseTwoChars :: Char -> Char -> Parser (Char, Char)
parseTwoChars c1 c2 = do
    a <- parseChar c1
    b <- parseChar c2
    return (a, b)

parseTwoChars' :: Char -> Char -> Parser (Char, Char)
parseTwoChars' c1 c2 = (,) <$> parseChar c1 <*> parseChar c2
