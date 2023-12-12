{-# LANGUAGE ApplicativeDo #-}
module Lessons.Lesson13 () where

import Control.Monad(when)
import Control.Concurrent ( threadDelay , forkIO)
import Control.Concurrent.Async ( async, wait, withAsync, Concurrently(..), runConcurrently, waitSTM, Async )

import Control.Monad.STM ( atomically, retry, STM )
import Control.Concurrent.STM.TVar
    ( newTVarIO, readTVar, readTVarIO, writeTVar, TVar, modifyTVar, newTVar )

import System.IO.Unsafe


action :: Int -> IO Int
action s = do
  threadDelay $ s * 1000 * 1000
  return s

main1 :: IO (Int, Int)
main1 =
  withAsync (action 3) $ \a1 -> do
  withAsync (action 3) $ \a2 -> do
    error "opa"
    r1 <- wait a1
    r2 <- wait a2
    let r = (r1, r2)
    putStrLn $ "R=" ++ show r
    return r

main2 :: IO (Int, Int, Int)
main2 = runConcurrently $ (,,)
    <$> Concurrently (action 5)
    <*> Concurrently (action 2)
    <*> Concurrently (action 3)

main3 :: IO (Int, Int, Int)
main3 = runConcurrently $ do
  a <- Concurrently (action 5)
  b <- Concurrently (action 2)
  c <- Concurrently (action 3)
  return (a, b, c)

transfer :: Int -> TVar Int -> TVar Int -> STM ()
transfer amount accA accB = do
  a <- (return $ unsafePerformIO $ putStrLn "ojoj" >> return 5) >>readTVar accA
  b <- readTVar accB
  writeTVar accA (a - amount)
  writeTVar accB (b + amount)
  after <- readTVar accA
  when (after < 0) retry

main4 :: IO ()
main4 = do
  a <- newTVarIO 40
  b <- newTVarIO 60
  as <- async $ atomically $ transfer 50 a b
  atomically $ modifyTVar a (+10)
  wait as
  readTVarIO a >>= print
  readTVarIO b >>= print