module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Either.Extra (maybeToEither)
import Data.List qualified as L
import InMemoryTables (database)
import Lib1 qualified
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = do
  liftIO $ putStrLn "Welcome to select-all-from database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = ["select", "*", "from"]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  case cmd' s of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> Either String String
    cmd' s = do
      table <- Lib1.parseSelectAllStatement c
      df <- maybeToEither ("Table not found: " ++ table) (Lib1.findTableByName database table)
      _ <- Lib1.validateDataFrame df
      return $ Lib1.renderDataFrameAsTable s df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final
