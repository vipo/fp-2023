
module Lessons.Lesson10 () where

import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT)
import Control.Monad.Trans.Except (ExceptT, throwE, runExceptT)
import Control.Monad.Trans.Class(lift)
import Control.Monad.IO.Class(liftIO)

import Data.IORef
import GHC.StableName (StableName)

d :: String
d = "foo"


mutate :: IO ()
mutate = do
    r <- newIORef "labas"
    readIORef r >>= putStrLn
    writeIORef r "medis"
    readIORef r >>= putStrLn


stateful :: State String Int
stateful = do
    value <- get
    let l = length value
    put "updated"
    return l

composed :: State String (Int, Int)
composed = do
    r1 <- stateful
    r2 <- stateful
    return (r1, r2)

type ParseError = String
type Parser a = ExceptT ParseError (State String) a

parseChar :: Char -> Parser Char
parseChar a = do
    inp <- lift get
    case inp of
        [] -> throwE "Empty input"
        (x:xs) -> if a == x then do
                                lift $ put xs
                                return a
                            else throwE ([a] ++ " expected but " ++ [x] ++ " found")

type Weird a = StateT String IO a

weird :: Weird Int
weird = do
    s1 <- get
    s2 <- lift getLine
    put "antaujinta"
    return (length s1 + length s2)

type Weirder a = StateT String (StateT Int IO) a

weirder :: Weirder Int
weirder = do
    s1 <- get
    s2 <- liftIO getLine
    put "antaujinta"
    return (length s1 + length s2)

newtype State' s a = State' {
    runState' :: s -> (a, s)
}

instance Functor (State' s)  where
  fmap :: (a -> b) -> State' s a -> State' s b
  fmap f functor = State' $ \inp -> 
    case runState' functor inp of
        (a, l) -> (f a, l)