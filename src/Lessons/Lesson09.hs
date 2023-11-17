{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveFunctor #-}
module Lessons.Lesson09 () where

import Control.Monad.Free (Free (..), liftF)
import Data.Time ( UTCTime, getCurrentTime )
import DataFrame ( DataFrame (DataFrame) , Column(..), ColumnType(..))

class Expr repr where
    lit :: Int -> repr
    add :: repr -> repr -> repr
    neg :: repr -> repr

instance Expr Int where
  add a b = a + b
  neg a = -a
  lit a = a

instance Expr String where
  add a b = a ++ " + " ++ b
  neg a = "-(" ++ a ++ ")"
  lit a = show a
    

class ExprMul repr where
    mul :: repr -> repr -> repr

instance ExprMul Int where
  mul a b = a * b

-- putStrLn :: String -> ()
-- getLine :: () -> String

data ConsoleAlgebra next
    = PutStrLn String (() -> next)
    | GetLine (String -> next)
    deriving Functor

type Console = Free ConsoleAlgebra

putStrLn' :: String -> Console ()
putStrLn' str = liftF $ PutStrLn str id

getLine' :: Console String
getLine' = liftF $ GetLine id

program :: Console String
program = do
    _ <- putStrLn' "Enter your name"
    name <- getLine'
    _ <- putStrLn' $ "Hello, " ++ name
    return name

runIO :: Console a -> IO a
runIO (Pure r) = return r
runIO (Free step) = do
    next <- runStep step
    runIO next
    where
        -- probably you will want to extend the interpreter
        runStep :: ConsoleAlgebra a -> IO a
        runStep (PutStrLn str next) = putStrLn str >> (return $ next ())
        runStep (GetLine next) = getLine >>= (\str -> return $ next str)
