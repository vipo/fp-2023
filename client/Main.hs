{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Data.Functor((<&>))
import Network.Wreq 
import Control.Lens ((^.), view)
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib4 qualified
import DataFrame
import InMemoryTables
import Network.Wreq
import Control.Lens
import Data.Aeson (ToJSON, FromJSON)
import Data.Yaml
import Network.Wreq
import GHC.Generics
import Control.Lens
import Data.Aeson (ToJSON, FromJSON)
import Data.Yaml
import Text.PrettyPrint
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy.Char8 as BSLC8
import qualified Data.ByteString.Lazy as BSL
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

data ServerResponse = ServerResponse
    { responseRight :: Maybe DataFrame
    , responseLeft :: Maybe ErrorMessage
    } deriving (Show, Eq, Generic)

type ErrorMessage = String

instance ToJSON ServerResponse
instance FromJSON ServerResponse


-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd input = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ sendQuery input
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right df -> do
        liftIO $ putStrLn $ Lib1.renderDataFrameAsTable s df
  where
    terminalWidth :: Integral n => Maybe (Window n) -> n
    terminalWidth = maybe 80 width



sendQuery :: String -> IO (Either ErrorMessage DataFrame)
sendQuery query = do
    let yamlPayload = encode query
    let headersList = [("Content-Type", "application/x-yaml")]
    httpResponse <- postWith (defaults & Network.Wreq.headers .~ headersList) "http://localhost:3000/query" yamlPayload
    let decodedResponse = case decodeEither (httpResponse ^. responseBody) of
                            Left decodeErr -> Left $ "Error decoding YAML: " ++ prettyPrintParseException decodeErr
                            Right r -> Right r
    case decodedResponse of
        Left errMsg -> return $ Left errMsg
        Right serverResp ->
            case (responseRight serverResp, responseLeft serverResp) of
                (Just df, _) -> return $ Right df
                (_, Just errMsg) -> return $ Left errMsg
                _ -> return $ Left "Invalid server response"
            
main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final