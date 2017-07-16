using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace NSQCore
{
    public static class HttpClientWrapper
    {
        public static  async Task<T> PostAsync<T>(HttpClient httpClient, SemaphoreSlim webClientLock, string url, byte[] data, Func<byte[], T> handler)
        {
            await webClientLock.WaitAsync().ConfigureAwait(false);

            try
            {
                var byteArrayContent = new ByteArrayContent(data);
                var response = await httpClient.PostAsync(url, byteArrayContent).ConfigureAwait(false);

                if (response.Content == null)
                    throw new Exception("null response");

                var byteArray = await response.Content.ReadAsByteArrayAsync();

                return handler(byteArray);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                webClientLock.Release();
            }
        }

        public static async Task<T> GetAsync<T>(HttpClient httpClient, SemaphoreSlim webClientLock, string url, Func<JObject, T> handler)
        {
            await webClientLock.WaitAsync().ConfigureAwait(false);
            try
            {
                string data = await httpClient.GetStringAsync(url).ConfigureAwait(false);

                var response = JObject.Parse(data);
                return response == null ? throw new Exception("invalid response") : handler(response);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                webClientLock.Release();
            }
        }
    }
}
