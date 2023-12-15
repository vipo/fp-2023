module Lib4() where
import Lib3
import Control.Monad.Trans.Class

type ColumnName = String

data ParsedStatement3 =
  SelectNow {}
  | DropTable {
    table :: TableName
  }
  | CreateTable {
    table :: TableName,
    newColumns :: [ColumnName]
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
    selectWhere :: Maybe WhereSelect
   }
  |Select {
    selectQuery :: SpecialSelect2,
    tables :: TableArray,
    selectWhere :: Maybe WhereSelect
  }
  | ShowTables { }
    deriving (Show, Eq)


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

throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err