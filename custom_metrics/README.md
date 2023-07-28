既存の`four_keys`リポジトリでは取り込めないデータを取り込むための資産を管理。

## Pipenvによる構築環境

```
# pipenvをインストール
pip install pipenv

# ライブラリインストール
cd custom_metrics
pipenv install
```

## 仮想環境　での実行
```
# 仮想環境に入る
pipenv shell

# 任意のスクリプトを実行
python hoge.py
```

pipenvに関わる詳細は以下を参照

https://qiita.com/y-tsutsu/items/54c10e0b2c6b565c887a
