{-# LANGUAGE InstanceSigs #-}

module Lessons.Lesson08 () where

data BValue = BInteger Integer
            | BString String
            | BList [BValue]
            | BDict [(String, BValue)]
            deriving Show

render :: BValue -> String
render (BInteger i) = concat ["i", show i, "e"]
render (BString s) = concat [show $ length s, ":", s]
render (BList l) = concat ["l", concatMap render l, "e"]
render (BDict d) = concat ["d", concatMap (\(k, v) -> render (BString k) ++ render v) d, "e"]

data User = User {
    name :: String,
    age :: Integer,
    metaData :: MetaData
} deriving Show

data MetaData = MetaData {
    tags :: [String],
    lastLogin :: Integer
} deriving Show

class ToBValue a where
    toBValue :: a -> BValue

instance ToBValue Integer where
    toBValue = BInteger

instance ToBValue MetaData where
    toBValue md = BDict [
        ("tags", BList $ map BString (tags md)),
        ("last_login", BInteger $ lastLogin md)
        ]

instance ToBValue User where
    toBValue :: User -> BValue
    toBValue u = BDict [
        ("name", BString $ name u),
        ("age", toBValue (age u)),
        ("meta_data", toBValue (metaData u))
        ]

user :: User
user = User "Vipo" 16 (MetaData ["fp", "hs"] 1000)


data Expression = Lit Integer
                | Add Expression Expression
                | Sub Expression Expression
                | Neg Expression
                deriving Show


program1 :: Expression
program1 = Neg (Add (Add (Lit 5) (Lit 4)) (Add (Lit 6) (Neg (Lit 1))))

eval :: Expression -> Integer
eval (Lit i) = i
eval (Neg e) = - (eval e)
eval (Add e1 e2) = eval e1 + eval e2
eval (Sub e1 e2) = eval e1 - eval e2

unPrettyPrint :: Expression -> String
unPrettyPrint (Lit i) = show i
unPrettyPrint (Neg e) = concat ["-(", unPrettyPrint e, ")"]
unPrettyPrint (Add e1 e2) = concat ["((", unPrettyPrint e1, ")+(", unPrettyPrint e2, "))"]
unPrettyPrint (Sub e1 e2) = concat ["((", unPrettyPrint e1, ")-(", unPrettyPrint e2, "))"]