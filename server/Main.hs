{-# LANGUAGE OverloadedStrings #-}

module Main(main) where -- module Main(main) where
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))


import Web.Scotty

import Network.HTTP.Types.Status
import Control.Concurrent.STM
import Data.Text.Encoding (decodeUtf8)
import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import qualified Data.ByteString.Lazy.Char8 as BS8
import Lib3 qualified
import Lib4 qualified
import Data.Text.Lazy (fromStrict, pack)
import DataFrame
import InMemoryTables
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import Lib2 (tableNameParser)
import System.Directory (doesFileExist, getDirectoryContents)
import System.FilePath (pathSeparator)
import Data.Yaml

type ThreadSafeTable = TVar (TableName, DataFrame)
type ThreadSafeDataBase = TVar [ThreadSafeTable]


data MyAppState = MyAppState
    { appDatabase :: ThreadSafeDataBase
    }

main :: IO ()
main = do
    table3 <- newTVarIO ("Table1", DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]])
    table2 <- newTVarIO ("Table2", DataFrame [Column "value" BoolType] [[BoolValue True], [BoolValue True], [BoolValue False]])

    -- Create individual TVar tables
    let tableList = [table3, table2]

    -- Create ThreadSafeTable from the list of TVar tables
    let initialDatabase = newTVarIO tableList

    -- Extract the TVar value from the IO action
    threadSafeDatabase <- initialDatabase

    scotty 3000 $ do
        post "/query" $ do
            requestBody <- body
            let parsed = Lib4.toStatement (BS8.unpack requestBody)
            case parsed of
              Just query -> do
                  executionResult <- liftIO $ runExecuteIO threadSafeDatabase $ Lib3.executeSql (Lib4.statement query)
                  liftIO $ print executionResult
                  fromTable ()
              Nothing -> do
                  let exception = Lib4.SqlException {Lib4.exception = "The query could not be decoded"}
                  let errorMessage = Lib4.fromException exception
                  text (pack errorMessage)

runExecuteIO :: ThreadSafeDataBase -> Lib3.Execution r -> IO r
runExecuteIO dataB (Pure r) = return r
runExecuteIO dataB (Free step) = do
    next <- runStep dataB step
    runExecuteIO dataB next
    where
        -- probably you will want to extend the interpreter
        runStep :: ThreadSafeDataBase -> Lib3.ExecutionAlgebra a -> IO a
        runStep dataB (Lib3.GetTime next) = getCurrentTime >>= return . next
        runStep dataB (Lib3.GetTables next) = do
          list <- getDirectoryContents "db"
          return $ next $ map (\str -> take ((length str) - 5) str)  $ init $ init list
        runStep dataB (Lib3.LoadFile tableName next) = do
          let filePath = toFilePath tableName
          exists <- doesFileExist filePath
          if exists
            then do
              fileContent <- readFile filePath
              return $ next $ Lib3.deserializedContent fileContent
            else return $ next $ Left $ "File '" ++ tableName ++ "' does not exist"
        runStep dataB (Lib3.SaveFile table next) = do
          case Lib3.serializedContent table of
            Right serializedContent -> writeFile (toFilePath (fst table)) serializedContent >>= return . next
            Left err -> error err
toFilePath :: String -> FilePath
toFilePath tableName = "db"++ [pathSeparator] ++ tableName ++ ".json" --".txt"
