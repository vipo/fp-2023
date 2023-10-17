module Lessons.Lesson07 () where

add :: Int -> Int -> Int
add a b =
    let
        _ = print a
    in
        a + b

-- add1 :: Int -> Int -> Int
-- add1 a b =
--     let
--         _ = print a
--         k = fmap length getLine
--     in
--         a + b + k


hello :: IO ()
hello = do
    putStrLn "Koks tavo vardas?"
    name <- getLine
    putStrLn $ "Labas, " ++ name