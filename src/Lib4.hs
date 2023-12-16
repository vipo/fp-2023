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

parseStatement3 :: String -> Either ErrorMessage ParsedStatement3
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
        Lib3.Insert _ _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Lib3.Update _ _ _-> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Lib3.Delete _ _ -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        Lib3.SelectNow -> case runParser stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        CreateTable _ _ -> case stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
        DropTable _  -> case stopParseAt rest of
          Left err2 -> Left err2
          Right _ -> Right query
    where
        p :: Parser ParsedStatement3
        p = showTableParser
               <|> showTablesParser
               <|> selectStatementParser
               <|> selectAllParser
               <|> insertParser
               <|> updateParser
               <|> deleteParser
               <|> selectNowParser
               <|> createTableParser
               <|> dropTableParser

dropTableParser :: Parser ParsedStatement3
dropTableParser = do
    _ <- queryStatementParser "drop"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser 
    pure $ DropTable table

createTableParser :: Parser ParsedStatement3
createTableParser = do
    _ <- queryStatementParser "create"
    _ <- whitespaceParser
    _ <- queryStatementParser "table"
    _ <- whitespaceParser
    table <- columnNameParser
    _ <- optional whitespaceParser
    _ <- queryStatementParser '('
    columnsAndTypes <- columnListParser
    _ <- optional parseWhitespace
    _ <- queryStatementParser ')'
    _ <- optional parseWhitespace
    pure $ CreateTableStatement table columnsAndTypes

columnListParser :: Parser [ColumnName]
columnListParser = seperate columnAndTypeParser (optional whitespaceParser >> char ',' *> optional whitespaceParser)

columnAndTypeParser :: Parser ColumnName
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