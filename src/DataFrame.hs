module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq)

data Column = Column String ColumnType
  deriving (Show, Eq)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq)
