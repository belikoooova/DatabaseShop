using System.Data;
using System.Runtime.Serialization;
using System.Text.Json;
using NewVariant.Exceptions;
using NewVariant.Interfaces;
using NewVariant.Models;
using Exception = System.Exception;

namespace DataLib;

public class DataBase: IDataBase
{
    /// <summary>
    /// Поле с таблицей базы данных.
    /// </summary>
    private Dictionary<Type, List<IEntity>> _dataBase = new Dictionary<Type, List<IEntity>>();
    
    /// <summary>
    /// Конструктор без параметров.
    /// </summary>
    public DataBase() { }
    
    /// <summary>
    /// Метод создает типизированную таблицу.
    /// </summary>
    /// <typeparam name="T"> Тип таблицы. </typeparam>
    /// <exception cref="DataBaseException"> Выбрасывается при ошибке при создании таблицы. </exception>
    public void CreateTable<T>() where T : IEntity
    {
        if (_dataBase.ContainsKey(typeof(T)))
        {
            throw new DataBaseException("Таблица данного типа уже существует.");
        }
        try
        {
            _dataBase.Add(typeof(T), new List<IEntity>());
        }
        catch (Exception e)
        {
            throw new DataBaseException("Произошла ошибка при создании таблицы.", e);
        }
    }

    /// <summary>
    /// Метод вставляет экземпляр в типизированную таблицу.
    /// </summary>
    /// <param name="getEntity"> Метод, создающий и возвращающий экземпляр. </param>
    /// <typeparam name="T"> Тип таблицы. </typeparam>
    /// <exception cref="DataBaseException"> Выбрасывается при ошибке при вставке таблицу. </exception>
    public void InsertInto<T>(Func<T> getEntity) where T : IEntity
    {
        if (!_dataBase.ContainsKey(typeof(T)))
        {
            throw new DataBaseException();
        }
        try
        {
            _dataBase[typeof(T)].Add(getEntity());
        }
        catch (Exception e)
        {
            throw new DataBaseException("Произошла ошибка при добавлении в таблицу.", e);
        }
    }

    /// <summary>
    /// Метод возвращает типизированную таблицу.
    /// </summary>
    /// <typeparam name="T"> Тип таблицы. </typeparam>
    /// <returns> Типизированная таблица. </returns>
    /// <exception cref="DataBaseException"> Выбрасывается при ошибке при возврате таблицы. </exception>
    public IEnumerable<T> GetTable<T>() where T : IEntity
    {
        if (!_dataBase.ContainsKey(typeof(T)))
        {
            throw new DataBaseException();
        }
        try
        {
            return CreateTypedList<T>();
        }
        catch (Exception e)
        {
            throw new DataBaseException("Произошла ошибка при возврате таблицы.", e);
        }
    }

    /// <summary>
    /// Метод сериализует типизированную таблицу.
    /// </summary>
    /// <param name="path"> Путь к файлу, в который сериадизуется таблица. </param>
    /// <typeparam name="T"> Тип таблицы. </typeparam>
    /// <exception cref="DataBaseException"> Выбрасывается при ошибке при сериализации. </exception>
    public void Serialize<T>(string path) where T : IEntity
    {
        if (!_dataBase.ContainsKey(typeof(T)))
        {
            throw new DataBaseException();
        }

        List<T> typedList = CreateTypedList<T>();

        try
        {
            using FileStream fs = new FileStream(path, FileMode.Create);
            {
                JsonSerializer.Serialize(fs, typedList);
            }
        }
        catch (IOException)
        {
            throw new DataBaseException("Ошибка ввода-вывода.", new IOException());
        }
        catch (SerializationException)
        {
            throw new DataBaseException("Ошибка сериализации.", new SerializationException());
        }
        catch (Exception e)
        {
            throw new DataBaseException("Произошла ошибка при сериализации.", e);
        }
    }

    /// <summary>
    /// Метод сериализует типизированную таблицу.
    /// </summary>
    /// <param name="path"> Путь к файлу, в который сериадизуется таблица. </param>
    /// <typeparam name="T"> Тип таблицы. </typeparam>
    /// <exception cref="DataBaseException"> Выбрасывается при ошибке при сериализации. </exception>
    public void Deserialize<T>(string path) where T : IEntity
    {
        List<T> typedList;
        try
        {
            using FileStream fs = new FileStream(path, FileMode.Open);
            {
                typedList = JsonSerializer.Deserialize<List<T>>(fs);
            }
            
            List<IEntity> list = new List<IEntity>();
            foreach (T item in typedList)
            {
                list.Add(item);
            }
        
            if (!_dataBase.ContainsKey(typeof(T)))
            {
                _dataBase.Add(typeof(T), list);
            }
            else
            {
                _dataBase[typeof(T)] = list;
            }
        }
        catch (IOException)
        {
            throw new DataBaseException("File input-output error!", new IOException());
        }
        catch (SerializationException)
        {
            throw new DataBaseException("Serialization error!", new SerializationException());
        }
        catch (Exception e)
        {
            throw new DataBaseException("Произошла ошибка при сериализации.", e);
        }
        
    }

    /// <summary>
    /// Метод приводит список интерсфейсных ссылок к списку ссылок Т.   
    /// </summary>
    /// <typeparam name="T"> Тип ссылок Т. </typeparam>
    /// <returns> Возвращает типизированный список. </returns>
    private List<T> CreateTypedList<T>()
    {
        List<T> typedList = new List<T>();
        foreach (IEntity entity in _dataBase[typeof(T)])
        {
            typedList.Add((T)entity);
        }

        return typedList;
    }
}