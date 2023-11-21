

module Lessons.Lesson11 () where

import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT)
import Control.Monad.Trans.Class(lift, MonadTrans)
import Control.Monad.IO.Class(liftIO, MonadIO)
import GHC.RTS.Flags (MiscFlags(installSEHHandlers))
import Type.Reflection (Module)
import Foreign.C (e2BIG)



newtype State' s a = State' {
    runState' :: s -> (a, s)
}

get' :: State' a a
get' = State' $ \s -> (s, s)

put' :: s -> State' s ()
put' a = State' $ const ((), a)

instance Functor (State' s)  where
  fmap :: (a -> b) -> State' s a -> State' s b
  fmap f functor = State' $ \inp ->
    case runState' functor inp of
        (a, l) -> (f a, l)


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

instance Monad m => Monad (EitherT e m) where
  (>>=) :: EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
  m >>= k = EitherT $ do
    eit <- runEitherT m
    case eit of
      Left e -> return $ Left e
      Right r -> runEitherT (k r)

data Student = Studen {
  name :: String
} deriving Show

throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err

type ParseError = String
type Parser a = EitherT ParseError (State String) a

parseChar :: Char -> Parser Char
parseChar a = do
    inp <- lift get
    case inp of
        [] -> throwE "Empty input"
        (x:xs) -> if a == x then do
                                lift $ put xs
                                return a
                            else throwE ([a] ++ " expected but " ++ [x] ++ " found")


type Weirder a = EitherT String IO a

instance MonadIO m => MonadIO (EitherT e m) where
  liftIO :: IO a -> EitherT e m a
  liftIO = lift . liftIO

calc :: Weirder String
calc = liftIO getLine

type Weirderer a = EitherT String (EitherT Int IO) a

calc2 :: Weirderer String
calc2 = do
  r <- liftIO getLine
  return r

-- instance Monad m => Functor (EitherT e m) where
--   fmap :: Monad m => (a -> b) -> EitherT e m a -> EitherT e m b
--   fmap f = EitherT . fmap (fmap f) . runEitherT

-- instance Monad m => Applicative (EitherT e m) where
--   pure :: Monad m => a -> EitherT e m a
--   pure = EitherT . return . Right
--   (<*>) :: Monad m => EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
--   EitherT fu <*> EitherT fa = EitherT $ do
--     u <- fu
--     case u of
--       Left e1 -> return $ Left e1
--       Right r1 -> do
--         a <- fa
--         case a of
--           Left e2 -> return $ Left e2
--           Right r2 -> return $ Right (r1 r2)

-- instance Monad m => Monad (EitherT e m) where
--   (>>=) :: Monad m => EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
--   ma >>= mf = EitherT $ do
--     a <- runEitherT ma
--     case a of
--         Left e1 -> return $ Left e1
--         Right r1 -> runEitherT $ mf r1

-- throwError :: Monad m => e -> EitherT e m a
-- throwError = EitherT . return . Left

-- type ParseError = String
-- type Parser a = EitherT ParseError (State String) a

-- instance MonadIO m => MonadIO (EitherT e m) where
--   liftIO :: IO a -> EitherT e m a
--   liftIO = lift . liftIO

-- type Weirder a = EitherT ParseError IO a

-- weirder :: Weirder String
-- weirder = do
--   liftIO getLine

-- parseChar :: Char -> Parser Char
-- parseChar a = do
--     inp <- lift get
--     case inp of
--         [] -> throwError "Empty input"
--         (x:xs) -> if a == x then do
--                                 lift $ put xs
--                                 return a
--                             else throwError ([a] ++ " expected but " ++ [x] ++ " found")

-- parsePair :: Char -> Parser (Char, Char)
-- parsePair c = do
--   v1 <- parseChar c
--   v2 <- parseChar c
--   return (v1, v2)
