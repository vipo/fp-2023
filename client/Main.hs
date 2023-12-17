module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Data.Functor((<&>))
import Network.Wreq 
import Control.Lens ((^.))
import Network.HTTP.Client
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib4 qualified
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


type Repl a = HaskelineT IO a
final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit


ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to client-server database! Press [TAB] for auto completion."


completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd inp = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- (Right <$> postWith defaults "http://localhost:1395" (Lib4.fromStatement (Lib4.SqlStatement {Lib4.statement = inp })))  --`E.catch` handleHttpException
      case df of 
        Left err -> return $ Left err
        Right maybeTable -> do 
          let table = Lib4.toTable $ maybeTable ^. Network.Wreq.responseBody :: Maybe Lib4.SqlTableFromYaml
          let isTable = Lib4.toDataframe table 
          case isTable of
            Just tableIs -> return $ Lib1.renderDataFrameAsTable s isTable
            Nothing -> return $ Left "The response contained of a bad table, we are extremely sorry"

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final