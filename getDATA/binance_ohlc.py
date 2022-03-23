import requests
import datetime
import time
import pandas as pd



# Essa função pega os dados da API da Binance 
# Ela vai calcular os intervalos de timestamp que vão ser pegos
# E realizar loops apra cada intervalo 
def getData(start,end,limit = 1000,step = 60):
    
    results = []
    jump = step * limit
    count = 0
    
    while(start < end):
        
        if(count == 0 or start > end):
            start = start
        else:
            start = start + jump
        
        stop = start + jump
        
        if(stop > end):
            stop = end
        
        print('Retrieving data from {} to {}'.format(start,stop))
        
        # Aqui são passados os parametros para a API da Binance 
        # Modificar o parâmetro "symbol" para o par necessário
        url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&limit="+str(limit)+"&interval=1h&startTime="+str(int(start *1000))+"&endTime="+str(int(stop * 1000))
        result = requests.get(url)
        print(len(result.json()))
        if(result.json() == []):
            print('Vazio')
        
        results += result.json()
        count += 1
        print(count)
        
        if(stop >= end):
            break;
        
    return results; 


# Modificar o start_time e o end_time conforme o requerido
# Os valores que estão sendo subtraidos servem para selecionar somente o minuto daquela data
start_time = datetime.datetime(2017,12,1,0,0)
start_time = time.mktime(start_time.timetuple()) - 10800

end_time = datetime.datetime(2022,1,10,0,0)
end_time = time.mktime(end_time.timetuple()) - 10800


res = getData(start_time, end_time)




# Passando a estrutura de um Dict para DataFrame
df = pd.DataFrame.from_dict(res)

df = df.drop(df.iloc[:, 6:], axis = 1)
df.columns = ['timestamp','Open','High','Low','Close','Volume']

df = df.set_index('timestamp')
df = df.set_index(pd.to_datetime(df.index, unit = "ms"))
df = df.apply(pd.to_numeric)

df = df.drop_duplicates()


# Código para retirar os valores nulos do dataset
nan_value = float("NaN")
df.replace("", nan_value, inplace=True)

df.dropna( inplace=True)


# Modificar o nome do arquivo
df.to_csv('./BTCUSDT(2017-2022)1h.csv')

