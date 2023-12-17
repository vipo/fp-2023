{-# LANGUAGE OverloadedStrings #-}

module Main(main) where -- module Main(main) where
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))


import Web.Scotty

import Network.HTTP.Types.Status
import Control.Concurrent.STM
import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import qualified Data.ByteString.Lazy.Char8 as BS8
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
import Lib2 (tableNameParser)
import System.Directory (doesFileExist, getDirectoryContents)
import System.FilePath (pathSeparator)
import Data.Yaml

type ThreadSafeTable = TVar (TableName, DataFrame)
type ThreadSafeDataBase = TVar [ThreadSafeTable]


data MyAppState = MyAppState
    { appDatabase :: ThreadSafeDataBase
    }

-- main :: IO ()
-- main = do
--     table1 <- newTVarIO ("Table1",  DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]])
--     table2 <- newTVarIO ("Table2", DataFrame [Column "value" BoolType][[BoolValue True],[BoolValue True],[BoolValue False]])

--     let initialDatabase = newTVarIO [table1, table2]

--     spockCfg <- defaultSpockCfg () PCNoDatabase (MyAppState <$> initialDatabase)

--     runSpock 1395 (spock spockCfg app)



main :: IO ()
main = do
    table1 <- newTVarIO ("Table1", DataFrame [Column "flag" StringType] [[StringValue "a"], [StringValue "b"]])
    table2 <- newTVarIO ("Table2", DataFrame [Column "value" BoolType] [[BoolValue True], [BoolValue True], [BoolValue False]])

    let initialDatabase = newTVarIO [table1, table2]

    scotty 1395 $ do
        post "/query" $ do
            requestBody <- body
            let parsed = Lib4.toStatement (BS8.unpack requestBody)
            return ()
            -- case parsed of
            --     Nothing -> json $ "Failed to decode SqlStatement"
                -- Just query -> do
                --     executionResult <- liftIO $ runExecuteIO tablesVar $ Lib3.executeSql query
                --     liftIO $ print executionResult
                --     json executionResult



-- app :: SpockM () () MyAppState ()
-- app = do
--   post root $ do
--     appState <- getState
--     mreq <- yamlBody
--     case mreq of
--       Just req -> do
--         result <- liftIO $ runExecuteIO (db appState) $ Lib3.executeSql $ query req
--         case result of
--           Left err -> do
--             setStatus badRequest400
--             Lib4.fromException Lib4.SqlException { Lib4.exception = err }
--           -- Right df -> do
--           --   fromTablee Lib4.Tablee { Lib4.dataFra = df }
--       Nothing -> do
--         setStatus badRequest400
--         Lib4.fromException Lib4.SqlException { Lib4.exception = "Request body format is incorrect." }
      
-- need this, need to change this 
runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        -- probably you will want to extend the interpreter
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
        runStep (Lib3.GetTables next) = do
          list <- getDirectoryContents "db"
          return $ next $ map (\str -> take ((length str) - 5) str)  $ init $ init list
        runStep (Lib3.LoadFile tableName next) = do
          let filePath = toFilePath tableName
          exists <- doesFileExist filePath
          if exists
            then do
              fileContent <- readFile filePath
              return $ next $ Lib3.deserializedContent fileContent
            else return $ next $ Left $ "File '" ++ tableName ++ "' does not exist"
        runStep (Lib3.SaveFile table next) = do
          case Lib3.serializedContent table of
            Right serializedContent -> writeFile (toFilePath (fst table)) serializedContent >>= return . next
            Left err -> error err
toFilePath :: String -> FilePath
toFilePath tableName = "db"++ [pathSeparator] ++ tableName ++ ".json" --".txt"