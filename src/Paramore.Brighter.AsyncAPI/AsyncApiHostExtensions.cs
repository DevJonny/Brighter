#region Licence
/* The MIT License (MIT)
Copyright © 2026 Jonny Olliff-Lee <jonny.ollifflee@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Neuroglia.AsyncApi;
using Neuroglia.AsyncApi.IO;
using Neuroglia.AsyncApi.v3;
using YamlDotNet.Serialization;

namespace Paramore.Brighter.AsyncAPI
{
    public static class AsyncApiHostExtensions
    {
        /// <summary>
        /// Generates the AsyncAPI document and writes it to both JSON and YAML files.
        /// The JSON file is written to <paramref name="outputPath"/>, and a corresponding
        /// YAML file is written alongside it (e.g. asyncapi.json → asyncapi.yaml).
        /// </summary>
        public static async Task<V3AsyncApiDocument> GenerateAsyncApiDocumentAsync(this IHost host, string outputPath, CancellationToken ct = default)
        {
            var generator = host.Services.GetService<IAmAnAsyncApiDocumentGenerator>();
            if (generator == null)
            {
                throw new InvalidOperationException(
                    "IAmAnAsyncApiDocumentGenerator is not registered. Call UseAsyncApi() on IBrighterBuilder during service configuration.");
            }

            var writer = host.Services.GetService<IAsyncApiDocumentWriter>();
            if (writer == null)
            {
                throw new InvalidOperationException(
                    "IAsyncApiDocumentWriter is not registered. Ensure UseAsyncApi() calls AddAsyncApiIO() during service configuration.");
            }

            var document = await generator.GenerateAsync(ct).ConfigureAwait(false);

            var directory = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            // Determine JSON file path. Normalize .yaml/.yml inputs so callers don't end up
            // with files like "asyncapi.yaml.json" — both formats are written, anchored on the
            // base name without extension.
            string jsonPath;
            if (outputPath.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                jsonPath = outputPath;
            }
            else if (outputPath.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase)
                  || outputPath.EndsWith(".yml", StringComparison.OrdinalIgnoreCase))
            {
                jsonPath = Path.ChangeExtension(outputPath, ".json");
            }
            else
            {
                jsonPath = $"{outputPath}.json";
            }
            var yamlPath = Path.ChangeExtension(jsonPath, ".yaml");

            // Serialize to JSON via the SDK in memory so we can splice in the x-* extensions
            // that are tracked outside the V3* SDK types (which have no Extensions bag).
            using var jsonBuffer = new MemoryStream();
            await writer.WriteAsync(document, jsonBuffer, AsyncApiDocumentFormat.Json, ct).ConfigureAwait(false);
            jsonBuffer.Position = 0;
            var rootNode = JsonNode.Parse(jsonBuffer) as JsonObject
                           ?? throw new InvalidOperationException("SDK writer emitted an unexpected document shape.");

            MergeExtensions(generator, rootNode);

            var mergedJson = rootNode.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(jsonPath, mergedJson, ct).ConfigureAwait(false);

            var yamlText = ConvertJsonNodeToYaml(rootNode);
            await File.WriteAllTextAsync(yamlPath, yamlText, ct).ConfigureAwait(false);

            return document;
        }

        private static void MergeExtensions(IAmAnAsyncApiDocumentGenerator generator, JsonObject root)
        {
            if (generator is not AsyncApiDocumentGenerator concrete) return;

            ApplyExtensionsAtPath(root, "channels", concrete.ChannelExtensions);
            ApplyExtensionsAtPath(root, "operations", concrete.OperationExtensions);

            if (root["components"] is JsonObject components)
            {
                ApplyExtensionsAtPath(components, "messages", concrete.MessageExtensions);
            }
        }

        private static void ApplyExtensionsAtPath(
            JsonObject parent,
            string sectionKey,
            IReadOnlyDictionary<string, IDictionary<string, object>> extensionsByKey)
        {
            if (extensionsByKey.Count == 0) return;
            if (parent[sectionKey] is not JsonObject section) return;

            foreach (var kv in extensionsByKey)
            {
                if (section[kv.Key] is not JsonObject target) continue;

                foreach (var ext in kv.Value)
                {
                    target[ext.Key] = ToJsonNode(ext.Value);
                }
            }
        }

        private static JsonNode? ToJsonNode(object? value)
        {
            return value switch
            {
                null => null,
                string s => JsonValue.Create(s),
                bool b => JsonValue.Create(b),
                int i => JsonValue.Create(i),
                long l => JsonValue.Create(l),
                short sh => JsonValue.Create(sh),
                byte by => JsonValue.Create(by),
                uint ui => JsonValue.Create(ui),
                ulong ul => JsonValue.Create(ul),
                ushort us => JsonValue.Create(us),
                sbyte sb => JsonValue.Create(sb),
                double d => JsonValue.Create(d),
                float f => JsonValue.Create(f),
                decimal m => JsonValue.Create(m),
                JsonNode jn => jn.DeepClone(),
                _ => JsonValue.Create(value.ToString())
            };
        }

        private static string ConvertJsonNodeToYaml(JsonNode root)
        {
            var tree = JsonNodeToObject(root);
            var serializer = new SerializerBuilder().Build();
            return serializer.Serialize(tree);
        }

        private static object? JsonNodeToObject(JsonNode? node)
        {
            switch (node)
            {
                case null:
                    return null;
                case JsonObject obj:
                {
                    var dict = new Dictionary<string, object?>(obj.Count);
                    foreach (var kv in obj)
                    {
                        dict[kv.Key] = JsonNodeToObject(kv.Value);
                    }
                    return dict;
                }
                case JsonArray arr:
                    return arr.Select(JsonNodeToObject).ToList();
                case JsonValue value:
                {
                    if (value.TryGetValue(out bool b)) return b;
                    if (value.TryGetValue(out long l)) return l;
                    if (value.TryGetValue(out double d)) return d;
                    if (value.TryGetValue(out string? s)) return s;
                    return value.ToJsonString();
                }
                default:
                    return node.ToJsonString();
            }
        }
    }
}
