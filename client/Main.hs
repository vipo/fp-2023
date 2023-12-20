{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Network.HTTP.Client
import Network.HTTP.Types
import Network.HTTP.Client
import Network.HTTP.Client.TLS

import Control.Lens
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Control.Exception (try, SomeException, IOException, handle, catch)

import Data.Functor ((<&>))
import Data.Time (UTCTime, getCurrentTime)
import Data.List qualified as L
import Data.Maybe
import Data.Text.Encoding (encodeUtf8)
import Data.Aeson (ToJSON, FromJSON)
import Data.Yaml
import qualified Data.ByteString.Lazy as BSL
import Data.Text (pack)
import Data.Aeson (ToJSON, FromJSON)
import Data.ByteString.Lazy.Char8 (unpack)

import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Lib4 qualified
import DataFrame
import InMemoryTables

import GHC.Generics

import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import System.Directory (doesFileExist, getDirectoryContents)
import System.FilePath (pathSeparator)

import Debug.Trace

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
                "set", "update", "delete", "Merry Christmas and Happy New Year"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

type ErrorMessage = String

-- Evaluation: handle each line user inputs
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
sendQuery query = catch
  (do
    let yamlData = BSL.fromStrict $ encodeUtf8 $ pack (Lib4.fromStatement (Lib4.SqlStatement {Lib4.statement = query}))
    initialRequest <- parseRequest "http://localhost:3000/query"
    let request = initialRequest
          { method = "POST"
          , requestBody = RequestBodyLBS yamlData
          , requestHeaders = [("Content-Type", "application/yaml")]
          }
    manager <- newManager tlsManagerSettings
    response <- httpLbs request manager
    handleResponse response
  )
  (\(e :: HttpException) -> do
    putStrLn $ "Caught HttpException: " ++ show e
    return $ Left $ "Http error occurred. Please check logs for details."
  )
  `catch`
  (\(e :: IOException) -> do
    putStrLn $ "Caught IOException: " ++ show e
    return $ Left $ "Connection error: Unable to connect to the server. Please check if the server is running."
  )
  `catch`
  (\(e :: SomeException) -> do
    putStrLn $ "Caught unexpected exception: " ++ show e
    return $ Left $ "Unexpected exception occurred. Please check logs for details."
  )

handleResponse :: Response BSL.ByteString -> IO (Either ErrorMessage DataFrame)
handleResponse response = do
  let status = responseStatus response
      responseBodyText = unpack (responseBody response)
  traceM $ "Response Body Text: " ++ responseBodyText
  case statusCode status of
    200 -> handleSuccess responseBodyText
    400 -> return $ Left "Bad request: Your query was not good."
    404 -> return $ Left "Not Found: The server could not find the requested resource."
    500 -> return $ Left "THIS WILL BE OUR ERROR WHEN GETTING BAD THINGS EYOOO"
    _   -> return $ Left $ "Unexpected status code: " ++ show status

handleSuccess :: String -> IO (Either ErrorMessage DataFrame)
handleSuccess responseBodyText = do
  let maybeTable = Lib4.toTable responseBodyText
  case maybeTable of
    Just table -> do
      let dataFrame = Lib4.toDataframe table
      return $ Right dataFrame
    Nothing ->
      return $ Left "Your request was successful, but the response did not have a table to show you, my dear"

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final
