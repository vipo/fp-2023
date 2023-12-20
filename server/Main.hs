{-# LANGUAGE OverloadedStrings #-}

module Main(main) where

import Web.Scotty
import Control.Concurrent.STM
import Network.HTTP.Types.Status
import Control.Concurrent (threadDelay, forkIO)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Control.Monad (void, forever)
import Data.Text.Encoding (decodeUtf8)
import Data.Text.Lazy (fromStrict, pack)
import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Data.Map qualified as M
import Data.Yaml
import Data.Either (rights)
import qualified Data.ByteString.Lazy.Char8 as BS8
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Internal as BSInternal

import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib4 qualified
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
import System.Directory (doesFileExist, getDirectoryContents, removeFile, listDirectory)
import System.FilePath (pathSeparator, takeBaseName, (</>))

type ThreadSafeTable = TVar (TableName, DataFrame)
type ThreadSafeDataBase = TVar [ThreadSafeTable]

main :: IO ()
main = do
    threadSafe <- loadSafeDataBase 
    threadSafeDatabase <- initTables threadSafe
    _ <- saveTablesEverySecond threadSafeDatabase
    scotty 3000 $ do
        post "/query" $ do
          requestBody <- body
          let requestBodyStrict = BS.toStrict requestBody
          let parsed = Lib4.toStatement (BS8.unpack requestBody)
          case parsed of
              Just query -> do
                  executionResult <- liftIO $ runExecuteIO threadSafeDatabase $ Lib4.executeSql (Lib4.statement query)
                  case executionResult of
                      Right res -> do
                        let result = Lib4.fromTable (Lib4.fromDataFrame res)
                        text (pack result)   
                      Left err -> do
                        status badRequest400
                        let result = Lib4.fromException (Lib4.SqlException {Lib4.exception = err})
                        text (pack result)   
              Nothing -> do
                  let exception = Lib4.SqlException {Lib4.exception = "The query could not be decoded"}
                  let errorMessage = Lib4.fromException exception
                  text (pack errorMessage)

runExecuteIO :: ThreadSafeDataBase -> Lib4.Execution r -> IO r
runExecuteIO dataB (Pure r) = return r
runExecuteIO dataB (Free step) = do
    next <- runStep step
    runExecuteIO dataB next
    where
        runStep :: Lib4.ExecutionAlgebra a -> IO a
        runStep (Lib4.GetTime next) = do
            getCurrentTime >>= return . next
        runStep (Lib4.GetTables next) =
          getAllTables dataB >>= return . next
        runStep (Lib4.LoadFile tableName next) = do
            dbVar <- atomically $ newTVar dataB
            tableList <- convertToThreadSafeTableList dbVar
            table <- seeIfExistsAndReturnTable tableName tableList
            case table of
                Just tableExists -> do
                    readTVarIO tableExists >>= (return . next . Right)
                Nothing -> do
                    return $ next $ Left $ "File '" ++ tableName ++ "' does not exist."
        runStep (Lib4.SaveFile table next) = do
            dbVar <- atomically $ newTVar dataB
            tableList <- convertToThreadSafeTableList dbVar
            tableIs <- seeIfExistsAndReturnTable (fst table) tableList
            case tableIs of
                Just tableRef -> do
                    atomically $ writeTVar tableRef table >>= return . next
                Nothing -> do
                    tableRefs <- readTVarIO dataB
                    newTableRef <- newTVarIO table
                    atomically $ writeTVar dataB $ newTableRef : tableRefs
                    return $ next ()
        runStep (Lib4.DropFile tableName next) = do
            dbVar <- atomically $ newTVar dataB
            tableList <- convertToThreadSafeTableList dbVar
            table <- seeIfExistsAndReturnTable tableName tableList
            case table of
                Nothing -> do
                    return $ next $ Left $ "Table '" ++ tableName ++ "' does not exist."
                Just ref -> do
                    tableRefs <- readTVarIO dataB
                    atomically $ writeTVar dataB $ filter (/= ref) tableRefs
                    removeFile $ toFilePath tableName
                    return $ next $ Right ()


convertToThreadSafeTableList :: TVar ThreadSafeDataBase -> IO [ThreadSafeTable]
convertToThreadSafeTableList dbVar = do
  db <- atomically $ readTVar dbVar
  atomically $ readTVar db

toFilePath :: String -> FilePath
toFilePath tableName = "db"++ [pathSeparator] ++ tableName ++ ".json" 

loadSafeDataBase :: IO Lib2.Database
loadSafeDataBase = do 
    list <- listFilesWithDir "db"
    tables <- mapM loadTable list
    let validTables = rights tables
    return validTables
    where
        loadTable :: FilePath -> IO (Either ErrorMessage (TableName, DataFrame))
        loadTable tablePath = do
            fileContent <- readFile tablePath
            case Lib3.deserializedContent fileContent of 
                Left err -> do
                    return $ Left $ "Error loading table " ++ tablePath ++ ": " ++ show err
                Right table -> do
                    return $ Right table

listFilesWithDir :: FilePath -> IO [FilePath]
listFilesWithDir dirPath = do
  entries <- listDirectory dirPath
  return [dirPath </> entry | entry <- entries]

initTables :: [(TableName, DataFrame)] -> IO ThreadSafeDataBase
initTables content = do
  tvars <- mapM (\(name, frame) -> newTVarIO (name, frame)) content
  tvarList <- newTVarIO tvars
  return tvarList

type ErrorMessage = String

saveTablesEverySecond :: ThreadSafeDataBase -> IO ()
saveTablesEverySecond db = do 
    _ <- forkIO $ foreverLoop
    return ()
  where
    foreverLoop :: IO ()
    foreverLoop = forever $ do
        threadDelay 1000000
        tables <- readTVarIO db
        saveTablesEverySecondRecursion tables

    saveTablesEverySecondRecursion :: [ThreadSafeTable] -> IO ()
    saveTablesEverySecondRecursion [] = pure ()  -- Handle the empty list case
    saveTablesEverySecondRecursion (x:xs) = do
        table <- readTVarIO x
        case Lib4.serializedContent table of
            Left err -> do
                error err
            Right serializedContent -> do
                let filePath = toFilePath (fst table)
                writeFile filePath serializedContent
                saveTablesEverySecondRecursion xs

getAllTables :: ThreadSafeDataBase -> IO [TableName]
getAllTables base = do
  db <- readTVarIO base
  getTheListRecursion db []
  where
    getTheListRecursion :: [ThreadSafeTable] -> [TableName] -> IO [TableName]
    getTheListRecursion [] acc = return acc
    getTheListRecursion (x:xs) acc = do
      table <- readTVarIO x
      getTheListRecursion xs (fst table : acc)


seeIfExistsAndReturnTable :: TableName -> [ThreadSafeTable] -> IO (Maybe ThreadSafeTable)
seeIfExistsAndReturnTable _ [] = return Nothing
seeIfExistsAndReturnTable name (x:xs) = do
  table <- readTVarIO x
  if fst table == name
    then return $ Just x
    else seeIfExistsAndReturnTable name xs
