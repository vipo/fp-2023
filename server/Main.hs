module Main() where -- module Main(main) where
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Web.Spock
import Web.Spock.Config
import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
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

main :: IO ()
main =
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