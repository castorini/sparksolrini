package cs848.collection;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * An interface that provides {@link Segment}s, which are containers holding {@link SourceDocument}s.
 * Typically, each implementation is associated with a particular {@link DocumentCollection}.
 *
 * @param <T> type of the source document
 */
public interface SegmentProvider<T extends SourceDocument> {
    /**
     * Returns a list of paths corresponding to file segments in the collection. Note that this method
     * returns paths, as opposed to actual file segments because each object is backed by an open file,
     * and thus having too many file handles open may be problematic for large collections. Use
     * {@link #createFileSegment(Path)} to instantiate a file segment object from its path.
     *
     * @return a list of paths corresponding to file segments in the collection
     */
    List<Path> getFileSegmentPaths();

    /**
     * Creates a {@code Segment} from a path.
     *
     * @param p path
     * @return {@code Segment} with the specified path
     * @throws IOException if file access error encountered
     */
    Segment<T> createFileSegment(Path p) throws IOException;
}