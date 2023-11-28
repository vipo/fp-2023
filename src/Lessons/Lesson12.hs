module Lessons.Lesson12 () where
import Control.Exception (bracket, Exception, throw)
import System.IO (openFile, hClose, IOMode(ReadMode), hClose)
import Data.Data (Typeable)

import Control.Concurrent
import Control.Concurrent.Chan

data MyException = ThisException | ThatException
    deriving Show

instance Exception MyException

foo :: IO Integer
foo = bracket (openFile "LICENSE" ReadMode)
  (\h -> putStrLn "Closing" >> hClose h)
  (\h -> error "42")

main1 :: IO ()
main1 = do
  chan <- newChan 
  forkIO $ do
    putStrLn "Labas"
    threadDelay $ 4 * 1000 * 1000
    writeChan chan 42
  val <- readChan chan
  putStrLn $ show val
